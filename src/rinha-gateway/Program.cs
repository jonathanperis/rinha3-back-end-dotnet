using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Http.Json;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);

// Env/config
var dbConnectionString = Environment.GetEnvironmentVariable("DB_CONNECTION_STRING")
    ?? "Host=postgres;Port=5432;Database=rinha;Username=postgres;Password=postgres;Pooling=true;Minimum Pool Size=10;Maximum Pool Size=50;Connection Pruning Interval=3;Max Auto Prepare=256;Auto Prepare Min Usages=2";
var defaultBaseUrl = Environment.GetEnvironmentVariable("PP_DEFAULT_BASEURL")
    ?? "http://payment-processor-default:8080";
var fallbackBaseUrl = Environment.GetEnvironmentVariable("PP_FALLBACK_BASEURL")
    ?? "http://payment-processor-fallback:8080";
var httpTimeoutMs = int.TryParse(Environment.GetEnvironmentVariable("HTTP_TIMEOUT_MS"), out var t) ? t : 300;

// JSON: source-gen + camelCase
builder.Services.Configure<JsonOptions>(o =>
{
    o.SerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
    o.SerializerOptions.TypeInfoResolverChain.Insert(0, AppJsonSerializerContext.Default);
    o.SerializerOptions.TypeInfoResolverChain.Add(AppJsonSerializerContext.Default);
});

// Npgsql DataSource (AutoPrepare via connection string in Npgsql 9)
builder.Services.AddNpgsqlDataSource(dbConnectionString);

// Ensure schema
builder.Services.AddHostedService<DbSchemaInitializer>();

// HttpClients
builder.Services.AddHttpClient("pp-default", client =>
{
    client.BaseAddress = new Uri(defaultBaseUrl);
    client.Timeout = TimeSpan.FromMilliseconds(httpTimeoutMs);
})
.ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler
{
    AutomaticDecompression = DecompressionMethods.None,
    AllowAutoRedirect = false,
    MaxConnectionsPerServer = 512,
    PooledConnectionLifetime = TimeSpan.FromMinutes(2),
    PooledConnectionIdleTimeout = TimeSpan.FromSeconds(30),
    ConnectTimeout = TimeSpan.FromMilliseconds(200),
    UseCookies = false
});

builder.Services.AddHttpClient("pp-fallback", client =>
{
    client.BaseAddress = new Uri(fallbackBaseUrl);
    client.Timeout = TimeSpan.FromMilliseconds(httpTimeoutMs);
})
.ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler
{
    AutomaticDecompression = DecompressionMethods.None,
    AllowAutoRedirect = false,
    MaxConnectionsPerServer = 512,
    PooledConnectionLifetime = TimeSpan.FromMinutes(2),
    PooledConnectionIdleTimeout = TimeSpan.FromSeconds(30),
    ConnectTimeout = TimeSpan.FromMilliseconds(200),
    UseCookies = false
});

// Health monitor respecting 1 call / 5s per service
builder.Services.AddSingleton<ProcessorsHealthState>();
builder.Services.AddHostedService<ProcessorsHealthPoller>();

// Data access
builder.Services.AddSingleton<PaymentsRepository>();

var app = builder.Build();

// POST /payments
app.MapPost("/payments", async (PaymentIn req, PaymentsRepository repo, IHttpClientFactory httpFactory, ProcessorsHealthState health, ILoggerFactory lf) =>
{
    if (req.CorrelationId == Guid.Empty || req.Amount <= 0)
        return Results.BadRequest();

    // Idempotency gate: if already processed, return 200
    var exists = await repo.ExistsAsync(req.CorrelationId);
    if (exists) return Results.Ok();

    var now = DateTime.UtcNow;

    // Choose a single target based on health cache (no dual-sending to avoid duplicates)
    ProcessorTarget target = ProcessorTarget.None;
    var h = health.Read();
    if (h.DefaultHealthy) target = ProcessorTarget.Default;
    else if (h.FallbackHealthy) target = ProcessorTarget.Fallback;

    if (target == ProcessorTarget.None)
        return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);

    var client = httpFactory.CreateClient(target == ProcessorTarget.Default ? "pp-default" : "pp-fallback");

    var forward = new ProcessorPaymentIn(req.CorrelationId, req.Amount, now);

    HttpResponseMessage resp;
    try
    {
        resp = await client.PostAsJsonAsync("/payments", forward, AppJsonSerializerContext.Default.ProcessorPaymentIn);
    }
    catch (OperationCanceledException)
    {
        // Timeout/cancel -> do NOT fallback to avoid double processing uncertainty
        return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
    }
    catch (Exception ex)
    {
        lf.CreateLogger("gateway").LogError(ex, "forward error");
        return Results.StatusCode(StatusCodes.Status502BadGateway);
    }

    // Accept only 2xx as success; otherwise bubble a 5xx/4xx
    if ((int)resp.StatusCode is >= 200 and < 300)
    {
        await repo.InsertAsync(req.CorrelationId, req.Amount, now, target);
        return Results.Ok();
    }

    // Do not try the other processor – keep single-target semantics
    return Results.StatusCode((int)resp.StatusCode);
})
.WithName("Payments");

// GET /payments-summary?from&to
app.MapGet("/payments-summary", async (DateTime? from, DateTime? to, PaymentsRepository repo) =>
{
    var summary = await repo.GetSummaryAsync(from, to);
    return Results.Json(summary, AppJsonSerializerContext.Default.PaymentsSummaryOut);
})
.WithName("PaymentsSummary");

// Simple liveness (serve plain text; avoid JSON serialization)
app.MapGet("/healthz", () => Results.Text("ok", "text/plain"));

app.Run();

enum ProcessorTarget : short { None = -1, Default = 0, Fallback = 1 }

sealed class DbSchemaInitializer(NpgsqlDataSource ds, ILogger<DbSchemaInitializer> logger) : IHostedService
{
    public async Task StartAsync(CancellationToken ct)
    {
        await using var cmd = ds.CreateCommand(@"
        CREATE TABLE IF NOT EXISTS processed_payments (
            correlation_id UUID PRIMARY KEY,
            amount NUMERIC NOT NULL,
            requested_at TIMESTAMPTZ NOT NULL,
            processor SMALLINT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS ix_processed_payments_requested_at ON processed_payments (requested_at);
        CREATE INDEX IF NOT EXISTS ix_processed_payments_processor ON processed_payments (processor);
        ");
        await cmd.ExecuteNonQueryAsync(ct);
        logger.LogInformation("Schema ensured");
    }

    public Task StopAsync(CancellationToken ct) => Task.CompletedTask;
}

sealed class PaymentsRepository(NpgsqlDataSource ds)
{
    public async Task<bool> ExistsAsync(Guid id, CancellationToken ct = default)
    {
        await using var cmd = ds.CreateCommand("SELECT 1 FROM processed_payments WHERE correlation_id = @id LIMIT 1;");
        cmd.Parameters.AddWithValue("id", id);
        await using var rdr = await cmd.ExecuteReaderAsync(ct);
        return await rdr.ReadAsync(ct);
    }

    public async Task InsertAsync(Guid id, decimal amount, DateTime requestedAtUtc, ProcessorTarget target, CancellationToken ct = default)
    {
        await using var cmd = ds.CreateCommand(@"
        INSERT INTO processed_payments (correlation_id, amount, requested_at, processor)
        VALUES (@id, @amount, @requested_at, @processor);");
        cmd.Parameters.AddWithValue("id", id);
        cmd.Parameters.AddWithValue("amount", decimal.Round(amount, 2));
        cmd.Parameters.AddWithValue("requested_at", requestedAtUtc);
        cmd.Parameters.AddWithValue("processor", (short)target);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<PaymentsSummaryOut> GetSummaryAsync(DateTime? from, DateTime? to, CancellationToken ct = default)
    {
        await using var cmd = ds.CreateCommand(@"
        WITH q AS (
        SELECT processor, amount
        FROM processed_payments
        WHERE (@from IS NULL OR requested_at >= @from)
            AND (@to   IS NULL OR requested_at <= @to)
        ),
        agg AS (
        SELECT
            SUM(CASE WHEN processor = 0 THEN 1 ELSE 0 END)::int AS default_count,
            COALESCE(SUM(CASE WHEN processor = 0 THEN amount ELSE 0 END), 0) AS default_amount,
            SUM(CASE WHEN processor = 1 THEN 1 ELSE 0 END)::int AS fallback_count,
            COALESCE(SUM(CASE WHEN processor = 1 THEN amount ELSE 0 END), 0) AS fallback_amount
        FROM q
        )
        SELECT default_count, default_amount, fallback_count, fallback_amount FROM agg;");
        cmd.Parameters.AddWithValue("from", from.HasValue ? from.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("to", to.HasValue ? to.Value : DBNull.Value);
        await using var rdr = await cmd.ExecuteReaderAsync(ct);
        await rdr.ReadAsync(ct);
        var defCount = rdr.GetInt32(0);
        var defAmount = rdr.GetDecimal(1);
        var fbCount = rdr.GetInt32(2);
        var fbAmount = rdr.GetDecimal(3);

        return new PaymentsSummaryOut(
            new SummaryPart(defCount, defAmount),
            new SummaryPart(fbCount, fbAmount));
    }
}

// Health polling

sealed class ProcessorsHealthState
{
    // Volatile allowed for reference types; swap snapshot atomically
    private volatile HealthSnapshot _snapshot = new(true, false, 0, false, 0, DateTime.MinValue);

    public HealthSnapshot Read() => _snapshot;

    public void Update(bool defaultHealthy, int defaultMinMs, bool fallbackHealthy, int fallbackMinMs)
    {
        _snapshot = new HealthSnapshot(
            HasValue: true,
            DefaultHealthy: defaultHealthy,
            DefaultMinResponseMs: defaultMinMs,
            FallbackHealthy: fallbackHealthy,
            FallbackMinResponseMs: fallbackMinMs,
            UpdatedAtUtc: DateTime.UtcNow);
    }

    public sealed record class HealthSnapshot(
        bool HasValue,
        bool DefaultHealthy,
        int DefaultMinResponseMs,
        bool FallbackHealthy,
        int FallbackMinResponseMs,
        DateTime UpdatedAtUtc);
}

sealed class ProcessorsHealthPoller(ILogger<ProcessorsHealthPoller> logger, IHttpClientFactory httpFactory, ProcessorsHealthState state)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var defaultClient = httpFactory.CreateClient("pp-default");
        var fallbackClient = httpFactory.CreateClient("pp-fallback");

        // Initial optimistic state
        state.Update(defaultHealthy: true, defaultMinMs: 0, fallbackHealthy: true, fallbackMinMs: 0);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var (dOk, dMin) = await Probe(defaultClient, stoppingToken);
                var (fOk, fMin) = await Probe(fallbackClient, stoppingToken);

                state.Update(dOk, dMin, fOk, fMin);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "health poll error");
                // keep last state
            }

            // Respect 1 request / 5s per service; poll every 5s
            await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
        }
    }

    private static async Task<(bool ok, int minMs)> Probe(HttpClient c, CancellationToken ct)
    {
        try
        {
            using var resp = await c.GetAsync("/payments/service-health", ct);
            if (resp.StatusCode == HttpStatusCode.TooManyRequests)
            {
                // Keep last known; treat as OK unknown
                return (true, 0);
            }
            if (!resp.IsSuccessStatusCode)
                return (false, 0);

            var data = await resp.Content.ReadFromJsonAsync(AppJsonSerializerContext.Default.ServiceHealthOut, ct);

            return (!data.Failing, data.MinResponseTime);
        }
        catch
        {
            return (false, 0);
        }
    }
}

// Models + STJ source-gen

public readonly record struct PaymentIn(Guid CorrelationId, decimal Amount);

public readonly record struct ProcessorPaymentIn(Guid CorrelationId, decimal Amount, DateTime RequestedAt);

public readonly record struct ServiceHealthOut(bool Failing, int MinResponseTime);

public readonly record struct SummaryPart(int TotalRequests, decimal TotalAmount);

// Avoid source-gen bug on property named "default"
public sealed record class PaymentsSummaryOut
{
    [JsonPropertyName("default")]
    public SummaryPart Default { get; init; }

    [JsonPropertyName("fallback")]
    public SummaryPart Fallback { get; init; }

    public PaymentsSummaryOut(SummaryPart @default, SummaryPart fallback)
    {
        Default = @default;
        Fallback = fallback;
    }
}

[JsonSerializable(typeof(PaymentIn))]
[JsonSerializable(typeof(ProcessorPaymentIn))]
[JsonSerializable(typeof(ServiceHealthOut))]
[JsonSerializable(typeof(SummaryPart))]
[JsonSerializable(typeof(PaymentsSummaryOut))]
public sealed partial class AppJsonSerializerContext : JsonSerializerContext { }
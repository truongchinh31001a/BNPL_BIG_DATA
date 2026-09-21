param(
    [int[]]$Sizes = @(100000, 500000, 1000000, 2000000),
    [int[]]$WorkerCounts = @(1, 2),
    [int]$Runs = 3
)

$ErrorActionPreference = "Stop"
$packages = "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.3"

docker compose stop fake-bnpl-producer spark-streaming-job spark-streaming-prediction
if ($LASTEXITCODE -ne 0) { throw "Unable to stop streaming services" }

foreach ($workers in $WorkerCounts) {
    docker compose up -d --scale "spark-worker=$workers" spark-worker
    if ($LASTEXITCODE -ne 0) { throw "Unable to scale Spark to $workers worker(s)" }

    foreach ($mode in @("full", "incremental")) {
        # One unreported warm-up per worker/mode combination primes Spark, Delta, and S3A.
        $cases = @(@{ Size = 100000; Run = 0 })
        foreach ($size in $Sizes) {
            foreach ($run in 1..$Runs) {
                $cases += @{ Size = $size; Run = $run }
            }
        }

        foreach ($case in $cases) {
            Write-Host "BENCHMARK workers=$workers mode=$mode size=$($case.Size) run=$($case.Run)"
            docker compose exec -T `
                -e "BENCHMARK_SIZE=$($case.Size)" `
                -e "BENCHMARK_WORKER_COUNT=$workers" `
                -e "BENCHMARK_RUN_NUMBER=$($case.Run)" `
                -e "BENCHMARK_MODE=$mode" `
                airflow-webserver `
                spark-submit `
                --master spark://spark-master:7077 `
                --packages $packages `
                --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension `
                --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog `
                /opt/airflow/spark/jobs/benchmark_scalability.py
            if ($LASTEXITCODE -ne 0) {
                throw "Benchmark failed: workers=$workers mode=$mode size=$($case.Size) run=$($case.Run)"
            }
        }
    }
}

Write-Host "Benchmark matrix completed."

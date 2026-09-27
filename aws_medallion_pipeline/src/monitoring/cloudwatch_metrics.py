import argparse
from datetime import datetime, timezone

import boto3


cloudwatch = boto3.client("cloudwatch")


def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--pipeline-name",
        required=True,
        help="Nome lógico do pipeline"
    )

    parser.add_argument(
        "--stage",
        required=True,
        help="Etapa monitorada, por exemplo Silver ou Gold"
    )

    parser.add_argument(
        "--records-processed",
        type=float,
        default=0
    )

    parser.add_argument(
        "--duration-seconds",
        type=float,
        default=0
    )

    parser.add_argument(
        "--duplicate-count",
        type=float,
        default=0
    )

    parser.add_argument(
        "--null-count",
        type=float,
        default=0
    )

    parser.add_argument(
        "--freshness-minutes",
        type=float,
        default=0
    )

    parser.add_argument(
        "--status",
        choices=["SUCCESS", "WARNING", "FAILED"],
        required=True
    )

    return parser.parse_args()


def status_to_value(status: str) -> float:
    mapping = {
        "SUCCESS": 1.0,
        "WARNING": 0.5,
        "FAILED": 0.0
    }

    return mapping[status]


def put_metrics(
    pipeline_name,
    stage,
    records_processed,
    duration_seconds,
    duplicate_count,
    null_count,
    freshness_minutes,
    status
):
    dimensions = [
        {
            "Name": "PipelineName",
            "Value": pipeline_name
        },
        {
            "Name": "Stage",
            "Value": stage
        }
    ]

    timestamp = datetime.now(timezone.utc)

    cloudwatch.put_metric_data(
        Namespace="DataEngineering/MedallionPipeline",
        MetricData=[
            {
                "MetricName": "RecordsProcessed",
                "Dimensions": dimensions,
                "Timestamp": timestamp,
                "Value": records_processed,
                "Unit": "Count"
            },
            {
                "MetricName": "PipelineDuration",
                "Dimensions": dimensions,
                "Timestamp": timestamp,
                "Value": duration_seconds,
                "Unit": "Seconds"
            },
            {
                "MetricName": "DuplicateCount",
                "Dimensions": dimensions,
                "Timestamp": timestamp,
                "Value": duplicate_count,
                "Unit": "Count"
            },
            {
                "MetricName": "NullCount",
                "Dimensions": dimensions,
                "Timestamp": timestamp,
                "Value": null_count,
                "Unit": "Count"
            },
            {
                "MetricName": "FreshnessMinutes",
                "Dimensions": dimensions,
                "Timestamp": timestamp,
                "Value": freshness_minutes,
                "Unit": "None"
            },
            {
                "MetricName": "PipelineStatus",
                "Dimensions": dimensions,
                "Timestamp": timestamp,
                "Value": status_to_value(status),
                "Unit": "None"
            }
        ]
    )


def main():
    args = get_args()

    put_metrics(
        pipeline_name=args.pipeline_name,
        stage=args.stage,
        records_processed=args.records_processed,
        duration_seconds=args.duration_seconds,
        duplicate_count=args.duplicate_count,
        null_count=args.null_count,
        freshness_minutes=args.freshness_minutes,
        status=args.status
    )

    print(
        f"Métricas enviadas para CloudWatch "
        f"| pipeline={args.pipeline_name} "
        f"| stage={args.stage} "
        f"| status={args.status}"
    )


if __name__ == "__main__":
    main()
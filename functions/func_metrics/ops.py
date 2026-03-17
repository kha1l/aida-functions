from dagster import OpExecutionContext, op


@op(required_resource_keys={"db", "rabbitmq"})
def metrics_op(context: OpExecutionContext):
    """TODO: реализовать операцию metrics."""
    # Пример: context.resources.rabbitmq.publish(payload)
    return None

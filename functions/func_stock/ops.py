from dagster import OpExecutionContext, op


@op(required_resource_keys={"db", "rabbitmq"})
def stock_op(context: OpExecutionContext):
    """TODO: реализовать операцию stock."""
    # Пример: context.resources.rabbitmq.publish(payload)
    return None

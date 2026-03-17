from dagster import OpExecutionContext, op


@op(required_resource_keys={"db", "rabbitmq"})
def revenue_op(context: OpExecutionContext):
    """TODO: реализовать операцию revenue."""
    # Пример: context.resources.rabbitmq.publish(payload)
    return None

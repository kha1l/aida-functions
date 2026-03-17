from dagster import OpExecutionContext, op


@op(required_resource_keys={"db", "rabbitmq"})
def tariff_op(context: OpExecutionContext):
    """TODO: реализовать операцию tariff."""
    # Пример: context.resources.rabbitmq.publish(payload)
    return None

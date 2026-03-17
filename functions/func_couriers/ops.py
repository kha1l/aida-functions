from dagster import OpExecutionContext, op


@op(required_resource_keys={"db", "rabbitmq"})
def couriers_op(context: OpExecutionContext):
    """TODO: реализовать операцию couriers."""
    # Пример: context.resources.rabbitmq.publish(payload)
    return None

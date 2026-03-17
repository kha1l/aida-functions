from dagster import OpExecutionContext, op


@op(required_resource_keys={"db", "rabbitmq"})
def birthday_op(context: OpExecutionContext):
    """TODO: реализовать операцию birthday."""
    # Пример: context.resources.rabbitmq.publish(payload)
    return None

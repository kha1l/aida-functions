from dagster import OpExecutionContext, op


@op(required_resource_keys={"db", "rabbitmq"})
def staff_op(context: OpExecutionContext):
    """TODO: реализовать операцию staff."""
    # Пример: context.resources.rabbitmq.publish(payload)
    return None

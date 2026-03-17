from dagster import OpExecutionContext, op


@op(required_resource_keys={"db", "rabbitmq"})
def stationary_op(context: OpExecutionContext):
    """TODO: реализовать операцию stationary."""
    # Пример: context.resources.rabbitmq.publish(payload)
    return None

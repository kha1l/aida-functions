from dagster import OpExecutionContext, op


@op(required_resource_keys={"db", "rabbitmq"})
def tickets_op(context: OpExecutionContext):
    """TODO: реализовать операцию tickets."""
    # Пример: context.resources.rabbitmq.publish(payload)
    return None

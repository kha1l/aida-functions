from dagster import OpExecutionContext, op


@op(required_resource_keys={"db", "rabbitmq"})
def refusal_op(context: OpExecutionContext):
    """TODO: реализовать операцию refusal."""
    # Пример: context.resources.rabbitmq.publish(payload)
    return None

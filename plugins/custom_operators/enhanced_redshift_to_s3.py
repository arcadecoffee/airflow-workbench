from airflow.providers.amazon.aws.transfers.redshift_to_s3 import RedshiftToS3Operator


class EnhancedRedshiftToS3Operator(RedshiftToS3Operator):

    _enhanced_template_fields = {'_select_query'}
    _enhanced_template_ext = {'sql.jinja2'}

    template_fields = RedshiftToS3Operator.template_fields + \
        tuple({f for f in _enhanced_template_fields if f not in RedshiftToS3Operator.template_fields})

    template_ext = RedshiftToS3Operator.template_ext + \
        tuple({f for f in _enhanced_template_ext if f not in RedshiftToS3Operator.template_ext})

    def __init__(self, *, iam_role: str = None, **kwargs) -> None:
        if iam_role and kwargs.get('aws_conn_id', None):
            raise ValueError('EnhancedRedshiftToS3Operator cannot be called with both iam_role and aws_conn_id.')
        super().__init__(**kwargs)
        self.select_query = self._select_query
        self.iam_role = iam_role

    def _build_unload_query(
            self, credentials_block: str, select_query: str, s3_key: str, unload_options: str
    ) -> str:
        if self.iam_role:
            credentials_block = f'aws_iam_role={self.iam_role}'
        return super()._build_unload_query(credentials_block, select_query, s3_key, unload_options)

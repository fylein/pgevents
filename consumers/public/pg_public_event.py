from common.event import PGEvent

class PGPublicEvent(PGEvent):
    tablename: str = None
    action: str = None
    old: dict = None
    new: dict = None
    id: str = None
    updated_at: str = None
    updated_by: dict = None
    diff: dict = None

    def skip_col(self, tablename, colname):
        # hack to ignore text_column1 etc columns - it is going to go away soon

        if 'public.transactions' in tablename:
            if '_column' in colname:
                return True
            if '_old' in colname:
                return True
            if 'custom_attributes' in colname:
                return True

        if 'public.users' in tablename:
            if 'password' in colname:
                return True
            if 'email' in colname:
                return True

        return False

    def clean_col_value(self, col):
        # pylint: disable=too-many-return-statements

        if col['value'] is None:
            return None

        if col['type'] in ['location']:
            return col['value'].replace('\\"', '"')

        if col['type'] in ['jsonb']:
            logger.info('calling __clean_jsonb_str for type jsonb')
            return self.clean_jsonb_str(val=col['value'])

        if col['name'] in ['extracted_data', 'custom_attributes', 'activity_details']:
            logger.info('calling __clean_jsonb_str name: %s', col['name'])
            return self.clean_jsonb_str(val=col['value'])

        if col['name'] in ['last_updated_by']:
            logger.info('calling __clean_jsonb_str name: %s', col['name'])
            res = self.clean_jsonb_str(val=col['value'])
            if isinstance(res, str):
                return {'org_user_id': res}
            return res

        return col['value']


    def loads(self, body):
        pl = json.loads(body)
        logger.debug('got payload %s', body)

        self.tablename = f"{pl['schema']}.{pl['table']}"
        self.action = pl['action']

        if self.action == 'I':
            self.new = self.clean_columns(self.tablename, pl['columns'])
            self.id = self.new.pop('id', None)
            self.updated_at = self.new.pop('updated_at', None)
            self.updated_by = self.new.pop('last_updated_by', None)
        elif self.action == 'U':
            self.new = self.clean_columns(self.tablename, pl['columns'])
            self.old = self.clean_columns(self.tablename, pl['identity'])
            self.id = self.new.pop('id', None)
            self.updated_at = self.new.pop('updated_at', None)
            self.updated_by = self.new.pop('last_updated_by', None)
            self.old.pop('last_updated_by', None)
            self.old.pop('updated_at', None)
            self.diff = self.diff_dict(self.old, self.new)
        elif self.action == 'D':
            self.old = self.clean_columns(self.tablename, pl['identity'])
            self.id = self.old.pop('id', None)
            self.updated_at = self.old.pop('updated_at', None)
            self.updated_by = self.old.pop('last_updated_by', None)
        else:
            raise NotImplemented('for action: %s'.format(self.action))

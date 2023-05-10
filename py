import json

class NoSQLDatabase:
    def __init__(self, db_path):
        self.db_path = db_path
        self.tables = {}
        self.load_database()

    def load_database(self):
        try:
            with open(self.db_path, 'r') as f:
                self.tables = json.load(f)
        except FileNotFoundError:
            self.save_database()

    def save_database(self):
        with open(self.db_path, 'w') as f:
            json.dump(self.tables, f)

    def create_table(self, table_name, schema):
        if table_name in self.tables:
            raise ValueError("Table already exists")
        self.tables[table_name] = {"schema": schema, "data": []}
        self.save_database()

    def get_table(self, table_name):
        if table_name not in self.tables:
            raise ValueError("Table not found")
        return self.tables[table_name]

    def insert_data(self, table_name, data):
        table = self.get_table(table_name)
        schema = table['schema']
        for key in schema:
            if key not in data:
                if schema[key]['nullable'] is False:
                    raise ValueError(f"{key} cannot be null")
                else:
                    data[key] = None
        if 'id' in schema:
            data['id'] = self.generate_id(table['data'])
        table['data'].append(data)
        self.save_database()

    def generate_id(self, data_list):
        if len(data_list) == 0:
            return 1
        else:
            last_id = data_list[-1]['id']
            return last_id + 1

    def update_data(self, table_name, id, data):
        table = self.get_table(table_name)
        schema = table['schema']
        for key in schema:
            if key in data:
                continue
            elif schema[key]['nullable'] is False:
                raise ValueError(f"{key} cannot be null")
            else:
                data[key] = None
        data['id'] = id
        data_list = table['data']
        for i in range(len(data_list)):
            if data_list[i]['id'] == id:
                data_list[i] = data
                self.save_database()
                return
        raise ValueError("Data not found")

    def delete_data(self, table_name, id):
        table = self.get_table(table_name)
        data_list = table['data']
        for i in range(len(data_list)):
            if data_list[i]['id'] == id:
                del data_list[i]
                self.save_database()
                return
        raise ValueError("Data not found")

    def filter_data(self, table_name, filters=None):
        table = self.get_table(table_name)
        data_list = table['data']
        if filters is None:
            return data_list
        else:
            filtered_data = []
            for data in data_list:
                match = True
                for key in filters:
                    if key not in data or data[key] != filters[key]:
                        match = False
                        break
                if match:
                    filtered_data.append(data)
            return filtered_data

    def sort_data(self, table_name, key, reverse=False):
        table = self.get_table(table_name)
        data_list = table['data']
        if key not in table['schema']:
            raise ValueError("Invalid key")
        data_list.sort(key=lambda x: x[key], reverse=reverse)
        return data_list

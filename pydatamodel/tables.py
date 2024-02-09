from dataclasses import dataclass, field, fields
from typing import List, Dict

@dataclass
class Table:
    name: str
    schema: str
    description: str = field(default=None)
    row_cnt: str = field(default=None, compare=False)
    columns: List = field(default_factory=list)
    native_types: bool = field(default=None, compare=False)
    id: str = field(compare=False)
    schema_id: str = field(compare=False)

    @property
    def primary_keys(self) -> List: 
        return [column for column in self.columns if column.primary]
    
    @classmethod
    def from_keboola(cls, keboola_json: Dict):
        return cls(
            id = keboola_json['id'],
            name = keboola_json['displayName'],
            schema = keboola_json['bucket']['displayName'],
            schema_id = keboola_json['bucket']['id'],
            description = next(iter([meta['value'] for meta in keboola_json.get('metadata') or [] if meta['key'] == 'KBC.description']), None),
            row_cnt = keboola_json.get('rowsCount'),
            columns = Column.from_keboola(keboola_json.get('columnMetadata'), keboola_json.get('primaryKey'), keboola_json.get('columns')),
            native_types = keboola_json.get('isTyped')
        )
    
    @classmethod
    def new_table(cls, name, schema_id, columns):
        return cls(
            id = None,
            name = name,
            schema = None,
            schema_id = schema_id,
            columns = columns
        )
    
    def __sub__(self, other):
        differences = {}
        for field in fields(self):
            if field.compare and getattr(self, field.name) != getattr(other, field.name):
                differences[field.name] = (getattr(self, field.name), getattr(other, field.name))
        
        if 'columns' in differences:
            col_dif = {}
            self_col_dict = {column.name: column for column in self.columns}
            other_col_dict = {column.name: column for column in other.columns}
            for col_name in self_col_dict:
                if col_name in other_col_dict:
                    dif = self_col_dict[col_name] - other_col_dict[col_name]
                    if dif:
                        col_dif[col_name] = dif
                else:
                    col_dif[col_name] = 'Left Only'
            for col_name in other_col_dict:
                if not col_name in self_col_dict:
                    col_dif[col_name] = 'Right Only'
            if col_dif:
                differences['columns'] = col_dif
            else:
                differences.pop('columns')
        
        return differences

@dataclass
class Column:
    name: str
    type: str
    description: str = field(default=None)
    primary: bool = field(default=False)
    length: str = field(default=None)

    @classmethod
    def from_keboola(cls, column_metadata, primary_columns, columns) -> List:
        cols = []
        if column_metadata:
            for name, metadata in column_metadata.items():
                cols.append(cls(
                    name = name,
                    type = next(iter([meta['value'] for meta in metadata if meta['key'] == 'KBC.datatype.basetype' and meta['provider'] == 'storage']), None),
                    length = next(iter([meta['value'] for meta in metadata if meta['key'] == 'KBC.datatype.length' and meta['provider'] == 'storage']), None),
                    description = next(iter([meta['value'] for meta in metadata if meta['key'] == 'KBC.description']), None),
                    primary = name in primary_columns
                ))
        elif columns:
            for name in columns:
                cols.append(cls(
                    name = name,
                    type = None,
                    length = None,
                    description = None,
                    primary = name in primary_columns,
                ))
        return cols
    
    def __sub__(self, other):
        differences = {}
        for field in fields(self):
            if field.compare and getattr(self, field.name) != getattr(other, field.name):
                differences[field.name] = (getattr(self, field.name), getattr(other, field.name))
        return differences
    
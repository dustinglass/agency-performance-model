from zipfile import ZipFile

from numpy import NaN
from pandas import read_csv, notnull
from sqlalchemy import create_engine


def extract_df(zip_path, csv_path):
    """Return a pandas.DataFrame of a CSV file nested in a ZIP folder."""    
    with ZipFile(zip_path) as zip_file:
        with zip_file.open(csv_path) as csv_file:
            return read_csv(csv_file)


def load_df(df, table_name, dbapi='sqlite:///insurance.db', 
            if_exists='replace'):
    """Insert data from `df` into table `table_name` in the database 
    identified by `dbapi`.
    """
    engine = create_engine(dbapi, echo=False)
    df.to_sql(table_name, con=engine, if_exists=if_exists, index=False)


def transform_df(fact_df, table_name, source_columns, id_column=None, 
                 drop_columns=None):
    """Return modified fact table `fact_df` and new dimension table, 
    both as pandas.DataFrame objects.
    """
    dim_df = fact_df.loc[:, source_columns]
    dim_df = dim_df.drop_duplicates()
    if id_column:
        dim_df = dim_df.rename({id_column: 'id'}, axis='columns')
    else:
        dim_df = dim_df.reset_index(drop=True)
        dim_df['id'] = dim_df.index + 1
        fact_df['{}_ID'.format(table_name.upper())] = fact_df.merge(
            dim_df, how='left', on=source_columns
            )['id']
    if not drop_columns:
        drop_columns = source_columns
    fact_df = fact_df.drop(drop_columns, axis='columns')
    return fact_df, dim_df


def main():
    """Insert raw Kaggle dataset into table "agency_performance", then
    clean and create star schema with the same dataset.
    """
    print('Loading raw dataset')
    df = extract_df('agencyperformance.zip', 'finalapi.csv')
    load_df(df, 'agency_performance', if_exists='append')
    
    print('Loading star schema')
    fact_df = df.copy()
    fact_df = fact_df.astype(object).where(notnull(df), None)
    fact_df, agency_df = transform_df(
        fact_df, 'agency', ['AGENCY_ID', 'PRIMARY_AGENCY_ID'], 
        id_column='AGENCY_ID', drop_columns=['PRIMARY_AGENCY_ID']
        )
    fact_df, product_df = transform_df(fact_df, 'product', 
                                       ['PROD_ABBR', 'PROD_LINE'])
    fact_df, state_df = transform_df(fact_df, 'state', ['STATE_ABBR'])
    fact_df, vendor_df = transform_df(fact_df, 'vendor', ['VENDOR'])
    fact_df['id'] = fact_df.index + 1
    load_df(fact_df, 'insurance')
    load_df(agency_df, 'agency')
    load_df(product_df, 'product')
    load_df(state_df, 'state')
    load_df(vendor_df, 'vendor')


if __name__ == '__main__':
    main()
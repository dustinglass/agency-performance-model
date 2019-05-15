from io import StringIO, BytesIO

from flask import Flask, request, send_file, jsonify
from flask_restful import Resource, Api
from pandas import read_sql
from sqlalchemy import create_engine

PARAM_TABLE_MAP = {
    'AGENCY_ID': 'insurance',
    'PRIMARY_AGENCY_ID': 'agency',
    'PROD_ABBR': 'product',
    'PROD_LINE': 'product',
    'STATE_ABBR': 'state',
    'VENDOR': 'vendor',
    'STAT_PROFILE_DATE_YEAR': 'insurance',
    'AGENCY_APPOINTMENT_YEAR': 'insurance',
    'PL_START_YEAR': 'insurance',
    'PL_END_YEAR': 'insurance',
    'COMMISIONS_START_YEAR': 'insurance',
    'COMMISIONS_END_YEAR': 'insurance',
    'CL_START_YEAR': 'insurance',
    'CL_END_YEAR': 'insurance',
    'ACTIVITY_NOTES_START_YEAR': 'insurance',
    'ACTIVITY_NOTES_END_YEAR': 'insurance',
}
JOIN_TEMPLATE = ' INNER JOIN {0} ON {0}.id = insurance.{1}_ID'
WHERE_TEMPLATE = "{}.{} = '{}'"

app = Flask(__name__)
api = Api(app)


class InvalidParameter(Exception):
    """Return descriptive parameter exception."""

    status_code = 422
    message_template = 'Invalid parameter `{}`.'

    def __init__(self, param=None, custom_message_template=None):
        Exception.__init__(self)
        if custom_message_template:
            self.message = custom_message_template.format(param)
        else:
            self.message = self.message_template.format(param)

    def to_dict(self):
        return {'message': self.message}


@app.errorhandler(InvalidParameter)
def handle_invalid_parameter(error):
    """Handle InvalidParameter exception in Flask app."""
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


def _select_df(sql, dbapi='sqlite:///insurance.db'):
    """Return pandas.DataFrame of result of SQL query `sql`."""
    engine = create_engine(dbapi, echo=False)
    return read_sql(sql, engine)


def _sql_response(sql, request_args):
    """Return API response based on SQL query `sql`."""
    data = _select_df(sql).to_dict(orient='records')
    return {'params': request_args, 'data': data}


def _build_out_sql(sql, request_args):
    """Return modified SQL query `sql` based on request parameters."""
    if bool(request_args):
        joins = set()
        where = []
        for param, value in request_args.items():
            table = PARAM_TABLE_MAP[param]
            if table != 'insurance':
                joins.add(JOIN_TEMPLATE.format(table, table.upper()))
            where.append(WHERE_TEMPLATE.format(table, param, value))
        sql += ''.join(joins)
        sql += ' WHERE {}'.format(' AND '.join(where))
    sql += ';'
    return sql


class Detail(Resource):
    """Return detailed information using different parameters."""
    
    def get(self):
        for param in request.args.keys():
            if param not in PARAM_TABLE_MAP:
                raise InvalidParameter(param)

        sql = 'SELECT insurance.* FROM insurance'   
        sql = _build_out_sql(sql, request.args)
        return _sql_response(sql, request.args)


class Summary(Resource):
    """Return summarized information using different parameters."""

    def get(self):
        for param in request.args.keys():
            if param not in PARAM_TABLE_MAP:
                raise InvalidParameter(param)

        sql = '''SELECT
            COUNT(*) AS RESULTS_COUNT, 
            SUM(RETENTION_POLY_QTY) AS RETENTION_POLY_QTY_SUM,
            SUM(POLY_INFORCE_QTY) AS POLY_INFORCE_QTY_SUM,
            SUM(PREV_POLY_INFORCE_QTY) AS PREV_POLY_INFORCE_QTY_SUM,
            SUM(NB_WRTN_PREM_AMT) AS NB_WRTN_PREM_AMT_SUM,
            SUM(WRTN_PREM_AMT) AS WRTN_PREM_AMT_SUM,
            SUM(PREV_WRTN_PREM_AMT) AS PREV_WRTN_PREM_AMT_SUM,
            SUM(PRD_ERND_PREM_AMT) AS PRD_ERND_PREM_AMT_SUM,
            SUM(PRD_INCRD_LOSSES_AMT) AS PRD_INCRD_LOSSES_AMT_SUM,
            AVG(RETENTION_RATIO) AS RETENTION_RATIO_AVG,
            AVG(LOSS_RATIO) AS LOSS_RATIO_AVG,
            AVG(LOSS_RATIO_3YR) AS LOSS_RATIO_3YR_AVG,
            AVG(GROWTH_RATE_3YR) AS GROWTH_RATE_3YR_AVG,
            SUM(CL_BOUND_CT_MDS) AS CL_BOUND_CT_MDS_SUM,
            SUM(CL_QUO_CT_MDS) AS CL_QUO_CT_MDS_SUM,
            SUM(CL_BOUND_CT_SBZ) AS CL_BOUND_CT_SBZ_SUM,
            SUM(CL_QUO_CT_SBZ) AS CL_QUO_CT_SBZ_SUM,
            SUM(CL_QUO_CT_EQT) AS CL_QUO_CT_EQT_SUM,
            SUM(PL_BOUND_CT_ELINKS) AS PL_BOUND_CT_ELINKS_SUM,
            SUM(PL_QUO_CT_ELINKS) AS PL_QUO_CT_ELINKS_SUM,
            SUM(PL_BOUND_CT_PLRANK) AS PL_BOUND_CT_PLRANK_SUM,
            SUM(PL_QUO_CT_PLRANK) AS PL_QUO_CT_PLRANK_SUM,
            SUM(PL_BOUND_CT_EQTTE) AS PL_BOUND_CT_EQTTE_SUM,
            SUM(PL_QUO_CT_EQTTE) AS PL_QUO_CT_EQTTE_SUM,
            SUM(PL_BOUND_CT_APPLIED) AS PL_BOUND_CT_APPLIED_SUM,
            SUM(PL_QUO_CT_APPLIED) AS PL_QUO_CT_APPLIED_SUM,
            SUM(PL_BOUND_CT_TRANSACTNOW) AS PL_BOUND_CT_TRANSACTNOW_SUM,
            SUM(PL_QUO_CT_TRANSACTNOW) AS PL_QUO_CT_TRANSACTNOW_SUM
        FROM INSURANCE''' 
        sql = _build_out_sql(sql, request.args)
        return _sql_response(sql, request.args)


class Report(Resource):
    """Return a CSV report with Premium info by Agency and Product Line
    using date range as parameters.

    Date range is inclusive, using PL_START_YEAR and PL_END_YEAR.
    """

    def get(self):
        sql = '''SELECT
            i.AGENCY_ID AS agency_id,
            p.PROD_LINE AS product_line,
            SUM(i.NB_WRTN_PREM_AMT) AS nb_wrtn_prem_amt_sum,
            SUM(i.WRTN_PREM_AMT) AS wrtn_prem_amt_sum,
            SUM(i.PREV_WRTN_PREM_AMT) AS prev_wrtn_prem_amt_sum,
            SUM(i.PRD_ERND_PREM_AMT) AS prd_ernd_prem_amt_sum
        FROM insurance i
        INNER JOIN product p ON p.id = i.PRODUCT_ID
        '''
        if bool(request.args):
            where = []
            for param, value in request.args.items():
                if param not in ['PL_START_YEAR', 'PL_END_YEAR']:
                    raise InvalidParameter(param)
                elif len(value) == 4 and value.isdigit():
                    if param == 'PL_START_YEAR':
                        where.append('i.PL_START_YEAR >= {}'.format(value))
                    else:
                        where.append('i.PL_END_YEAR <= {}'.format(value))
                else:
                    message = '{} must follow the format YYYY'
                    raise InvalidParameter(param, message)
            sql += ' WHERE {}'.format(' AND '.join(where))
        sql += ' GROUP BY i.AGENCY_ID, p.PROD_LINE;'

        df = _select_df(sql)
        s = StringIO()
        df.to_csv(s)
        b = BytesIO()
        b.write(s.getvalue().encode('utf-8'))
        b.seek(0)
        s.close()
        return send_file(b, mimetype = 'text/csv')


api.add_resource(Detail, '/detail')
api.add_resource(Summary, '/summary')
api.add_resource(Report, '/report')

if __name__ == '__main__':
    app.run(debug=True)
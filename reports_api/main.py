import os
from datetime import date
from typing import Optional

import clickhouse_connect
import jwt
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

KEYCLOAK_ISSUER = os.environ['KEYCLOAK_ISSUER']
KEYCLOAK_JWKS_URL = os.environ['KEYCLOAK_JWKS_URL']
REPORT_ROLE = 'prothetic_user'

REPORT_COLUMNS = [
    'report_date', 'prosthesis_id', 'prosthesis_model', 'movements_count', 'avg_reaction_ms',
    'max_reaction_ms', 'recognition_errors', 'avg_battery_level',
]

app = FastAPI(title='BionicPRO Reports API')
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get('CORS_ORIGINS', 'http://localhost:3000').split(','),
    allow_methods=['GET'],
    allow_headers=['Authorization'],
)

jwks_client = jwt.PyJWKClient(KEYCLOAK_JWKS_URL)
bearer = HTTPBearer(auto_error=False)


def clickhouse():
    return clickhouse_connect.get_client(
        host=os.environ['CLICKHOUSE_HOST'],
        port=int(os.environ.get('CLICKHOUSE_PORT', 8123)),
        username=os.environ['CLICKHOUSE_USER'],
        password=os.environ['CLICKHOUSE_PASSWORD'],
        database=os.environ.get('CLICKHOUSE_DB', 'reports'),
    )


def current_user(credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer)) -> str:
    if credentials is None:
        raise HTTPException(status_code=401, detail='Требуется аутентификация')
    try:
        signing_key = jwks_client.get_signing_key_from_jwt(credentials.credentials)
        claims = jwt.decode(
            credentials.credentials, signing_key.key, algorithms=['RS256'],
            issuer=KEYCLOAK_ISSUER, options={'verify_aud': False},
        )
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail='Недействительный токен')

    if REPORT_ROLE not in claims.get('realm_access', {}).get('roles', []):
        raise HTTPException(status_code=403, detail='Нет доступа к отчётам')
    return claims['preferred_username']


@app.get('/reports')
def get_report(
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    user_id: str = Depends(current_user),
):
    client = clickhouse()
    available_until = client.command('SELECT max(loaded_until) FROM etl_loaded_periods')
    available_until = date.fromisoformat(str(available_until))
    if available_until == date(1970, 1, 1):
        raise HTTPException(status_code=404, detail='Данные для отчётов ещё не подготовлены')

    period_to = min(date_to or available_until, available_until)
    period_from = date_from or date(1970, 1, 1)

    result = client.query(
        'SELECT report_date, prosthesis_id, prosthesis_model, movements_count, '
        'round(toFloat64(avg_reaction_ms), 1), max_reaction_ms, recognition_errors, '
        'round(toFloat64(avg_battery_level), 1) FROM reports_mart '
        'WHERE user_id = %(user_id)s AND report_date BETWEEN %(from)s AND %(to)s '
        'ORDER BY report_date, prosthesis_id',
        parameters={'user_id': user_id, 'from': period_from, 'to': period_to},
    )
    items = [dict(zip(REPORT_COLUMNS, row)) for row in result.result_rows]

    return {
        'user_id': user_id,
        'available_until': available_until,
        'period_truncated': date_to is not None and date_to > available_until,
        'items': items,
    }

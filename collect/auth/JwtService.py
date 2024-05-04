import os
import jwt
from dotenv import load_dotenv

load_dotenv()
secret_key = os.getenv('JWT_SECRET')


def decode_token(token: str):
    try:
        # json decode는 자동으로 현재 시간, exp date 비교해 만료 여부 확인해줌
        decoded_token = jwt.decode(token, secret_key, algorithms=["HS256"])
        # TODO 토큰에서 정보 가져오기
        # TODO 정확한 토큰인지 확인하기 (PROJECT, SERVICE)
        print(decoded_token)
    except jwt.ExpiredSignatureError:
        print("Token has expired")
    except jwt.InvalidTokenError:
        print("Invalid token")

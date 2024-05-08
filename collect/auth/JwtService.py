import base64
import hashlib
import hmac
import json
import os
import time

import jwt
from dotenv import load_dotenv

load_dotenv()
secret_key = os.getenv('JWT_SECRET')


def decode_token(token: str) -> bool:
    try:
        decoded_secret_key = base64.b64decode(secret_key)
        hmac_key = hmac.new(decoded_secret_key, digestmod=hashlib.sha256).digest()

        decoded_token = jwt.decode(token, hmac_key, algorithms=["HS256"])
        service_id = decoded_token.get("serviceId")

        if service_id is None:
            print("Invalid token: Missing required claims")
            return False

        print("Token is valid")
        print("Payload:")
        print(decoded_token)
        return True
    except jwt.ExpiredSignatureError:
        print("Token has expired")
        return False
    except jwt.InvalidTokenError:
        print("Token has invalid secret")
        return False


def get_payload_from_token(token: str) -> tuple:
    try:
        parts = token.split(".")
        if len(parts) != 3:
            raise ValueError("Invalid token format")

        payload_part = parts[1]
        payload_decoded = base64.urlsafe_b64decode(payload_part + "==")
        payload = json.loads(payload_decoded)

        service_id = payload.get("serviceId")
        project_id = payload.get("projectId")

        if service_id is None or project_id is None:
            raise ValueError("Missing required claims")

        return project_id,service_id
    except (ValueError, json.JSONDecodeError):
        print("Invalid token")
        return None, None


def create_jwt_for_service(service_id, expiration_ms):
    now_millis = int(time.time() * 1000)
    now_seconds = now_millis // 1000
    exp_seconds = (now_millis + expiration_ms) // 1000

    payload = {
        "sub": "ServiceToken",
        "serviceId": service_id,
        "iat": now_seconds,
        "exp": exp_seconds
    }

    decoded_secret_key = base64.b64decode(secret_key)
    hmac_key = hashlib.sha256(decoded_secret_key).digest()

    token = jwt.encode(payload, hmac_key, algorithm="HS256")
    return token


# jwt_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJTZXJ2aWNlVG9rZW4iLCJwcm9qZWN0SWQiOjgsInNlcnZpY2VJZCI6MiwiaWF0IjoxNzE1MTY2NzM0LCJleHAiOjIwMzA1MjY3MzR9.oKJenUKkyTR47cMZnpmY6OBO5MQGpMSU3r1g0QhEeZU"
# decode_token(jwt_token)
# print(create_jwt_for_service(8,2))

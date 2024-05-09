import base64
import hashlib
import os
import time

from dotenv import load_dotenv
from jose import jwt, JWTError

load_dotenv()
secret_key = os.getenv('JWT_SECRET')


def decode_token(token: str) -> tuple:
    try:
        secret_key_bytes = secret_key.encode('utf-8')
        decoded = base64.b64decode(secret_key_bytes)
        key = hashlib.sha256(decoded).digest()

        decoded_token = jwt.decode(token, key=decoded, algorithms=["HS256"], options={"verify_signature": True, "verify_aud": False, "verify_iat": False, "verify_exp": False, "verify_nbf": False, "verify_iss": False, "verify_sub": False, "require": []})
        project_id = decoded_token.get("projectId")
        service_id = decoded_token.get("serviceId")

        if service_id or project_id is None:
            print("Invalid token: Missing required claims")
            return None

        return project_id, service_id
    except jwt.ExpiredSignatureError:
        print("Token has expired")
        return None
    except JWTError:
        print("Token has invalid signature or is malformed")
        return None
    except jwt.InvalidAlgorithmError:
        print("Token has an invalid signing algorithm")
        return None
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None


def get_payload_from_token(message) -> tuple:
    project_id = 8
    service_id = 2
    return project_id, service_id


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

    token = jwt.encode(payload, secret_key, algorithm="HS256")
    return token


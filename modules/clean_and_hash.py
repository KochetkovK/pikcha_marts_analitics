import hashlib
import re
import secrets
import string

def clean_phone(phone: str) -> str:
    '''Функция приводит номер телефона к единому виду.
       Возвращает номер, состоящий только из цифр и начинается с восьмерки'''
    new_phone = phone.replace(' ', '').replace('(', '').replace(')', '').replace('-', '')
    if re.fullmatch(r'\+?\d{11}', new_phone):
        new_phone = re.sub(r'^\+7', '8', new_phone, 1)
        return new_phone
    else:
        return None
    
def clean_email(email: str) -> str:
    '''Функция очищает email'''
    if email:
        return email.replace(' ', '').lower()
    else:
        return None

    
def hash_and_sault(obj: str) -> str:
    ''' Функция возвращает хеш объекта, измененный солью'''
    # соль отдельно не сохраняем
    characters = string.ascii_uppercase + string.digits
    sault = ''.join(secrets.choice(characters) for _ in range(12))
 
    md5_hash_object = hashlib.md5((obj + sault).encode("utf-8")).hexdigest()

    return md5_hash_object

if __name__ == "__main__":
    
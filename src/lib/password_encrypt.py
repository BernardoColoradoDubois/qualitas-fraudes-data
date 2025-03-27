import bcrypt

def password_hash(password: str) -> str:

    # Convertir el string a bytes para bcrypt
    password_bytes = password.encode('utf-8')
    
    # Generar el salt y el hash
    salt = bcrypt.gensalt()
    hash_bytes = bcrypt.hashpw(password_bytes, salt)
    
    # Convertir el hash de bytes a string
    hash_str = hash_bytes.decode('utf-8')
    
    return hash_str



def password_compare(password: str, hashed_password: str) -> bool:

    # Convertir los strings a bytes para bcrypt
    password_bytes = password.encode('utf-8')
    hashed_password_bytes = hashed_password.encode('utf-8')
    
    # Verificar la contraseña
    return bcrypt.checkpw(password_bytes, hashed_password_bytes)
import bcrypt

password = "2B596807074ACA37C4BAEBD7695C8898029087986EE7A526AAAADDA6F26501E3"

bytes_password = bytes(password, 'utf-8')

brcrypt_hash = str(bcrypt.hashpw(bytes_password, bcrypt.gensalt(10)))

print(brcrypt_hash)
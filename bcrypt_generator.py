from src.lib.password_encrypt import password_hash, password_compare

password = "2B596807074ACA37C4BAEBD7695C8898029087986EE7A526AAAADDA6F26501E3"

hashed_password = password_hash(password)

print(hashed_password)

#'$2b$10$wp/lTBl5teP3hgdQ8MU7sOWMJfu31gQxR2jFOV0cH.spYlOnqJXAO'


print(password_compare(password+"lorem", hashed_password))


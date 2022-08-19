import sys

print(sys.executable)
print(sys.version)

user = {
    "name": {"first": "Damon", "last": "Cortesi"},
    "title": "AWS Analytics Dev Advocate",
}

match user:
    case {"name": {"first": first_name}}:
        pass

print(first_name)
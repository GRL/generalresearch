# THL Django App

This package contains the Django models and migrations required to create the
THL-compatible database schema. It is meant to be installed inside any Django
project so the schema can be applied automatically with `makemigrations` and
`migrate`.

---

## 1. Installation

Add the package to your environment:
(e.g. local development)
```bash
pip install generalresearch[django]
```

(e.g. editable install recommended during development)
```bash
pip install -e '/path/to/project/py-utils[django]'
```


## 2. Add the App to INSTALLED_APPS

In your Django test project's settings.py:

```
INSTALLED_APPS = [
    # ...
    "generalresearch.thl_django",
]
```

## 3. Test that it worked
```shell
python manage.py shell
```

# For use in Jenkins / pytest

There is a dummy/minimal django project under the `app` folder. This is set up
with thl_django as the only installed_app, and to read all setting from 
environment variables.

## Example Usage

```postgresql
-- postgres=# 
CREATE DATABASE "thl-jenkins" WITH TEMPLATE = template0 ENCODING = 'UTF8';
```

```shell
pip install generalresearch[django]
export DB_NAME=thl-jenkins
export DB_USER=postgres
export DB_PASSWORD=password
export DB_HOST=127.0.0.1
```
```shell
# Confirm imports worked
python -m generalresearch.thl_django.app.manage shell -v 2

> assert settings.DATABASES['default']['NAME'] == 'thl-jenkins'
```

```shell
# Migrate
python -m generalresearch.thl_django.app.manage migrate --noinput
```
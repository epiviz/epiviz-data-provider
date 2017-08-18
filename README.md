# Install
```
    # clone the package
    pip install -r requirements.txt
```

# Database and flask configurations

We use SQLAlchemy for database sessions. To configure the API to your instance, update the SQL 
parameters in the `config.py` script.

# Run epiviz web services
```
    python run.py
```

# Testing using Insomnia or postman
upload the `test_ws_client.json` to insomnia, postman or HAR to test the different end points 
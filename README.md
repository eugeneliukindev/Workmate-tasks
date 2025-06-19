# Installation dependencies
- Poetry:
    ```bash
    poetry install
    ```
- pip:
    ```bash
    pip install -r requirements.txt
    ```
  
# Examples to start script
```bash
python3 main.py data/products.csv --filter "name=iphone 14"
```
```bash
python3 main.py data/products.csv --filter "price>=149" \
                                  --filter "price<=299" \
                                  --agg "price=max" 
```

# Available operators and aggregators:
- Operators:
  - ">"
  - "<"
  - ">="
  - "<="
  - "="
  - "!="

- Aggregators:
  - "avg"
  - "min"
  - "max"
  - "sum"
  - "count"

# Run tests:
```bash
pytest
```

# Run mypy:
```bash
mypy --strict .
```
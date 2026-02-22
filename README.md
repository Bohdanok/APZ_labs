# APZ_labs


### python package installation for the environment
```bash
pip install -r requirements.txt
```

### check the mandatory task

```bash
cd regular_src
pytest test_basic.py -v
```

### check the bonus task

```bash
cd bonus_src
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. logging.proto
pytest test_bonus.py -v
```

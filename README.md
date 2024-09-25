1. run on local
python3 -m venv env
source env/bin/activate 
pip install -r requirements.txt

2. package to upload to lambdb

pip install --target ./package -r requirements.txt

cd package
zip -r ../my_deployment_package.zip .

cd ..
zip my_deployment_package.zip lambda_function.py global-bundle.pem
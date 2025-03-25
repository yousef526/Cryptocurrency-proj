
#from producer import main

def apiCall():

    import requests
    num = 0
    with open("/opt/airflow/dags/CryptoScripts/partNum.txt","r") as f:
      num = int(f.readline())
    currency = "USD"
    url = f"https://rest.coinapi.io/v1/exchangerate/{currency}"

    payload = {}
    headers = {
      'Accept': 'text/plain',
      'X-CoinAPI-Key': 'a0f0299c-a6fb-46df-a22a-ac9f21f33af3'
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    FILE_NAME = f"/opt/airflow/dags/CryptoScripts/demo{num}.json"

    with open(f"{FILE_NAME}","w") as f:
      f.write(response.text)


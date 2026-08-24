# SAP extension pattern

`example.py` demonstrates the boundary only. SAP RFC, OData, HANA, GUI automation and proprietary SDKs are different integrations and should receive concrete adapters when the real protocol is known. Credentials stay in Airflow Connections/Secrets Backend.

SAP GUI belongs closer to the isolated/RPA execution profile. Do not add a fake SAP SDK or a broad `SAPClient` to the core package.

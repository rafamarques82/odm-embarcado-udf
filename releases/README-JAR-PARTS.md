# JAR Dividido - ODM Embarcado UDF

Este JAR foi dividido em 3 partes usando ZIP split para facilitar o versionamento no Git.

## Arquivos:
- odm-embarcado-udf-1.0.0.z01 (13MB) - Parte 1
- odm-embarcado-udf-1.0.0.z02 (13MB) - Parte 2  
- odm-embarcado-udf-1.0.0.zip (11MB) - Parte 3 (final)

## Como extrair com 7zip:

### Windows:
1. Coloque todos os 3 arquivos (.z01, .z02, .zip) na mesma pasta
2. Clique com botão direito no arquivo .zip
3. Selecione "7-Zip > Extract Here"
4. O 7zip vai automaticamente juntar todas as partes e extrair o JAR

### Linux/Mac:
```bash
zip -F odm-embarcado-udf-1.0.0.zip --out odm-embarcado-udf-complete.zip
unzip odm-embarcado-udf-complete.zip
```

Ou simplesmente:
```bash
7z x odm-embarcado-udf-1.0.0.zip
```

## Arquivo resultante:
- odm-embarcado-udf-1.0.0.jar (~39MB)

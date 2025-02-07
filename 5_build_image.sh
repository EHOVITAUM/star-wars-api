# realiza o login na AWS 
aws ecr get-login-password --region SEU_REGIAO | docker login --username AWS --password-stdin SEU_ID_DA_CONTA.dkr.ecr.SEU_REGIAO.amazonaws.com

# cria um repositório de docker images
aws ecr create-repository --repository-name NOME_DO_REPOSITORIO

# cria e adiciona tags a imagem 
docker build -t NOME_DA_IMAGEM .
docker tag NOME_DA_IMAGEM:latest SEU_ID_DA_CONTA.dkr.ecr.SEU_REGIAO.amazonaws.com/NOME_DO_REPOSITORIO:latest

# sobe de fato a imagem na AWS 
docker push SEU_ID_DA_CONTA.dkr.ecr.SEU_REGIAO.amazonaws.com/NOME_DO_REPOSITORIO:latest

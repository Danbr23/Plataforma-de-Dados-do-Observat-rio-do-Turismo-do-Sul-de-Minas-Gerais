# dados/consts.py (crie este arquivo)
# índices baseados no layout dos "Estabelecimentos" (0-based)
ORDEM = {
    "Estabelecimentos":0,
    "Empresas":1,
    "Socios":2,
    "Simples":3,
    "Paises":4,
    "Municipios":5,
    "Naturezas":6,
    "Qualificacoes":7,
    "Motivos":8,
    "Cnaes":9,
}

IDX = {
    "Estabelecimentos":{
        "cnpj_basico": 0,
        "cnpj_ordem": 1,
        "cnpj_dv": 2,
        "ident_matriz_filial": 3,
        "nome_fantasia": 4,
        "situacao_cadastral":5,
        "data_situacao_cadastral":6,
        "motivo_situacao_cadastral":7,
        "nome_cidade_exterior":8,
        "codigo_pais":9,
        "data_inicio_atividade": 10,
        "cnae_fiscal_principal": 11,
        "cnae_fiscal_secundaria": 12,
        "tipo_logradouro":13,
        "logradouro":14,
        "numero":15,
        "complemento":16,
        "bairro":17,
        "cep":18,
        "uf": 19,
        "codigo_municipio_rf": 20,  # você citou ser a 21ª coluna (logo índice 20)
        "ddd1":21,
        "telefone1":22,
        "ddd2":23,
        "telefone2":24,
        "dddfax":25,
        "fax":26,
        "correio_eletronico":27,
        "situacao_especial":28,
        "data_situacao_especial":29,
    },
    "Empresas":{
        "cnpj_basico":0, 
        "razao_social":1,
        "natureza_juridica":2,
        "qualificacao_responsavel":3,
        "capital_social":4,
        "porte":5,
        "ente_federativo_responsavel":6,
    },
    "Socios":{
        "cnpj_basico":0,
        "identificador_socio":1,
        "nome":2,
        "cnpj_cpf_socio":3,
        "qualificacao":4,
        "data_entrada_sociedade":5,
        "pais":6,
        "cpf_representante_legal":7,
        "nome_representante_legal":8,
        "qualificacao_representante_legal":9,
        "faixa_etaria":10, 
    },
    "Simples":{
        "cnpj_basico":0,
        "opcao_pelo_simples":1,
        "data_opcao_simples":2,
        "data_exclusao_simples":3,
        "opcao_pelo_mei":4,
        "data_opcao_pelo_mei":5,
        "data_exclusao_mei":6,
    },
    "Paises":{
        "codigo":0,
        "descricao":1,
    },
    "Municipios":{
        "codigo":0,
        "descricao":1,
    },
    "Naturezas":{
        "codigo":0,
        "descricao":1,
    },
    "Qualificacoes":{
        "codigo":0,
        "descricao":1,
    },
    "Motivos":{
        "codigo":0,
        "descricao":1,
    },
    "Cnaes":{
        "codigo":0,
        "descricao":1,
    }

}
SELECT_ORDER = [
        [
            "cnpj_basico",
            "cnpj_ordem",
            "cnpj_dv",
            "ident_matriz_filial",
            "nome_fantasia",
            "situacao_cadastral",
            "data_situacao_cadastral",
            "motivo_situacao_cadastral",
            "nome_cidade_exterior",
            "codigo_pais",
            "data_inicio_atividade",
            "cnae_fiscal_principal",
            "cnae_fiscal_secundaria",
            "tipo_logradouro",
            "logradouro",
            "numero",
            "complemento",
            "bairro",
            "cep",
            "uf",
            "codigo_municipio_rf",  # você citou ser a 21ª coluna (logo índice 20)
            "ddd1",
            "telefone1",
            "ddd2",
            "telefone2",
            "dddfax",
            "fax",
            "correio_eletronico",
            "situacao_especial",
            "data_situacao_especial",
        ],
        [
            "cnpj_basico", 
            "razao_social",
            "natureza_juridica",
            "qualificacao_responsavel",
            "capital_social",
            "porte",
            "ente_federativo_responsavel"
        ],
        [
            "cnpj_basico",
            "identificador_socio",
            "nome",
            "cnpj_cpf_socio",
            "qualificacao",
            "data_entrada_sociedade",
            "pais",
            "cpf_representante_legal",
            "nome_representante_legal",
            "qualificacao_representante_legal",
            "faixa_etaria" 
        ],
        [
            "cnpj_basico",
            "opcao_pelo_simples",
            "data_opcao_simples",
            "data_exclusao_simples",
            "opcao_pelo_mei",
            "data_opcao_pelo_mei",
            "data_exclusao_mei",
        ],
        [
            "codigo",
            "descricao"
        ],
        [
            "codigo",
            "descricao"
        ],
        [
            "codigo",
            "descricao"
        ],
        [
            "codigo",
            "descricao"
        ],
        [
            "codigo",
            "descricao"
        ],
        [
            "codigo",
            "descricao"
        ]
    ]
DELIMITER = ";"
ENCODING = "latin-1"  # costuma vir assim nos dumps da Receita

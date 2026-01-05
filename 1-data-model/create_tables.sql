CREATE TABLE `EXPOSI_RISCO` (
  `cgeportfolio` bigint DEFAULT NULL,
  `nmfundo` text,
  `icvm175` text,
  `origem` text,
  `cd_cge_prestador` bigint DEFAULT NULL,
  `gestor` text,
  `dsemailgestorfundotofim` text,
  `dsemailgestorfundoccfim` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `TB_ENQ_CLASSE_CVM` (
  `portfolio` text,
  `data` text,
  `situacao` text,
  `calculo_cota` text,
  `cod_anbid` text,
  `conta` double DEFAULT NULL,
  `cod_serie` text,
  `cod_cge` double DEFAULT NULL,
  `cnpj` double DEFAULT NULL,
  `cod_selic` text,
  `cod_cetip` text,
  `mneumonico` text,
  `isin` text,
  `tipo_portfolio` text,
  `classe_cvm` text,
  `administrador` text,
  `controladoria` text,
  `gestor` text,
  `custodiante` text,
  `credito_privado` text,
  `segue_resolucao_de_efpc` text,
  `aplicacao_inicial` double DEFAULT NULL,
  `qualificacao_global` text,
  `grupo_gestor_global` text,
  `perfil_aprovacao_boletas` text,
  `nome_fundo_global` text,
  `data_genesis` text,
  `nome_completo_cic` text,
  `nickname_cic` text,
  `exclusivo` text,
  `classificacao_anbid_scp` text,
  `officer_fundo` text,
  `fundo_fechado` text,
  `despesa_migrada` text,
  `status_mercado` text,
  `calculo_de_premio` text,
  `data_fim` text,
  `obs_encerramento` text,
  `responsavel_ultima_cota` text,
  `motivo_pagamento_repasse` text,
  `cod_cblc` text,
  `restricao_investimento` text,
  `segue_resolucao_seguradora` text,
  `conta_sap` text,
  `destinado_a_rpps` text,
  `data_do_exercicio_social` text,
  `codigo_sti` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE `TB_ENQ_DE_PARA_COD_ATIVO` (
  `codTipoAtivo` bigint DEFAULT NULL,
  `descricao` text,
  `dt_carga` timestamp NULL DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE `TB_ENQ_ENTUBA_MARGEM` (
  `cgegestor` bigint DEFAULT NULL,
  `dataenvio` datetime DEFAULT NULL,
  `status` bigint DEFAULT NULL,
  `cnpj` bigint DEFAULT NULL,
  `cgeportfolio` bigint DEFAULT NULL,
  `nomeportfolio` text,
  `margemlocal` double DEFAULT NULL,
  `margemoffshore` double DEFAULT NULL,
  `metologiautilizada` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE `TB_ENQ_EXCECOES_MARGEM` (
  `cge` bigint DEFAULT NULL,
  `status` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE `TB_ENQ_EXPOSI_RISCO_SNAPSHOT` (
  `CgePortfolio` bigint NOT NULL,
  `origem` varchar(50) DEFAULT NULL,
  `dt_carga` datetime NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE `TB_ENQ_EXPOSI_RISCO_SNAPSHOT_BACKUP` (
  `id_carga` bigint NOT NULL AUTO_INCREMENT,
  `dt_carga` datetime NOT NULL,
  `dt_carteira` date NOT NULL,
  `CgePortfolio` bigint DEFAULT NULL,
  `NmFundo` varchar(255) DEFAULT NULL,
  `ICVM175` varchar(100) DEFAULT NULL,
  `origem` varchar(100) DEFAULT NULL,
  `CD_CGE_PRESTADOR` varchar(100) DEFAULT NULL,
  `GESTOR` varchar(255) DEFAULT NULL,
  `DsEmailGestorFundoToFim` varchar(500) DEFAULT NULL,
  `DsEmailGestorFundoCCFim` varchar(500) DEFAULT NULL,
  PRIMARY KEY (`id_carga`)
) ENGINE=InnoDB AUTO_INCREMENT=3607 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE `TB_ENQ_MARGEM_GESTOR_SNAPSHOT` (
  `id_carga` bigint NOT NULL AUTO_INCREMENT,
  `dt_carga` datetime NOT NULL,
  `CgeGestor` bigint DEFAULT NULL,
  `DataEnvio` datetime DEFAULT NULL,
  `Status` int DEFAULT NULL,
  `Cnpj` varchar(20) DEFAULT NULL,
  `CgePortfolio` bigint DEFAULT NULL,
  `NomePortfolio` varchar(255) DEFAULT NULL,
  `MargemLocal` decimal(20,6) DEFAULT NULL,
  `MargemOffshore` decimal(20,6) DEFAULT NULL,
  `MetologiaUtilizada` varchar(600) DEFAULT NULL,
  PRIMARY KEY (`id_carga`)
) ENGINE=InnoDB AUTO_INCREMENT=151363 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE `TB_ENQ_PL` (
  `data_processamento` datetime DEFAULT NULL,
  `data_posicao` datetime DEFAULT NULL,
  `cnpj` bigint DEFAULT NULL,
  `cge_fundo` bigint DEFAULT NULL,
  `fundo` text,
  `cge_gestor` bigint DEFAULT NULL,
  `nome_gestor` text,
  `publico_alvo` text,
  `tipo_portfolio` text,
  `classificacao_anbid_scp` text,
  `fundo_fechado` text,
  `restricao_investimento` text,
  `desc_classe_cvm` text,
  `criticidade` double DEFAULT NULL,
  `pl` double DEFAULT NULL,
  `margempl` double DEFAULT NULL,
  `conta_corrente` bigint DEFAULT NULL,
  `saldo_cc` double DEFAULT NULL,
  `expos_derpl` double DEFAULT NULL,
  `deriv_riscopl` double DEFAULT NULL,
  `liquidezpl` double DEFAULT NULL,
  `var_95` double DEFAULT NULL,
  `var_95_paris_3m` double DEFAULT NULL,
  `var_95_paris_1y` double DEFAULT NULL,
  `var_95_paris_2y` double DEFAULT NULL,
  `var_99` double DEFAULT NULL,
  `var_99_paris_3m` double DEFAULT NULL,
  `var_99_paris_1y` double DEFAULT NULL,
  `var_99_paris_2y` double DEFAULT NULL,
  `bull` double DEFAULT NULL,
  `bear` double DEFAULT NULL,
  `bull_paris` double DEFAULT NULL,
  `bear_paris` double DEFAULT NULL,
  `rentab_dia` double DEFAULT NULL,
  `rentab_mes` double DEFAULT NULL,
  `rentab_12m` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE `TB_ENQ_PL_HISTORICO` (
  `id_carga` bigint NOT NULL AUTO_INCREMENT,
  `cgePortfolio` bigint NOT NULL,
  `data` date NOT NULL,
  `patrimonio_abertura` decimal(20,2) DEFAULT NULL,
  `patrimonio_fechamento` decimal(20,2) DEFAULT NULL,
  `dt_carga` datetime NOT NULL,
  PRIMARY KEY (`id_carga`)
) ENGINE=InnoDB AUTO_INCREMENT=2311732 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE `TB_ENQ_PL_SNAPSHOT` (
  `id_carga` bigint NOT NULL AUTO_INCREMENT,
  `dt_carga` datetime NOT NULL,
  `sucesso` tinyint DEFAULT NULL,
  `erros` text,
  `cgePortfolio` bigint DEFAULT NULL,
  `dataProcessamento` datetime DEFAULT NULL,
  `dataPosicao` date DEFAULT NULL,
  `nomeFundo` varchar(255) DEFAULT NULL,
  `cnpj` varchar(20) DEFAULT NULL,
  `cgeGestor` bigint DEFAULT NULL,
  `nomeGestor` varchar(255) DEFAULT NULL,
  `publicoAlvo` varchar(100) DEFAULT NULL,
  `tipoPortfolio` varchar(100) DEFAULT NULL,
  `classAnbidScp` varchar(100) DEFAULT NULL,
  `fundoFechado` tinyint DEFAULT NULL,
  `restricaoInvestimento` varchar(255) DEFAULT NULL,
  `classeCvm` varchar(50) DEFAULT NULL,
  `descClasseCvm` varchar(100) DEFAULT NULL,
  `pl` decimal(20,2) DEFAULT NULL,
  `margemPl` decimal(20,2) DEFAULT NULL,
  `contaCorrente` decimal(20,2) DEFAULT NULL,
  `saldoCc` decimal(20,2) DEFAULT NULL,
  `derivativos` decimal(20,2) DEFAULT NULL,
  `derivativosRisco` decimal(20,2) DEFAULT NULL,
  `derivativosPl` decimal(20,2) DEFAULT NULL,
  `derivativosRiscoPl` decimal(20,2) DEFAULT NULL,
  `liquidezPl` decimal(20,2) DEFAULT NULL,
  `rentabilidadeDia` decimal(10,6) DEFAULT NULL,
  `rentabilidadeMes` decimal(10,6) DEFAULT NULL,
  `rentabilidadeAno` decimal(10,6) DEFAULT NULL,
  `criticidade` varchar(50) DEFAULT NULL,
  `var95` decimal(20,6) DEFAULT NULL,
  `var99` decimal(20,6) DEFAULT NULL,
  `bull` decimal(20,6) DEFAULT NULL,
  `bear` decimal(20,6) DEFAULT NULL,
  `bullParis` decimal(20,6) DEFAULT NULL,
  `bearParis` decimal(20,6) DEFAULT NULL,
  `var95Paris3M` decimal(20,6) DEFAULT NULL,
  `var95Paris1Y` decimal(20,6) DEFAULT NULL,
  `var95Paris2Y` decimal(20,6) DEFAULT NULL,
  `var99Paris3M` decimal(20,6) DEFAULT NULL,
  `var99Paris1Y` decimal(20,6) DEFAULT NULL,
  `var99Paris2Y` decimal(20,6) DEFAULT NULL,
  `fiiFiq` decimal(20,6) DEFAULT NULL,
  PRIMARY KEY (`id_carga`)
) ENGINE=InnoDB AUTO_INCREMENT=204506 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE `TB_ENQ_POSICOES_FUNDOS_EXPOSTOS` (
  `Nickname` varchar(150) DEFAULT NULL,
  `DataCarteira` date DEFAULT NULL,
  `notional` decimal(20,6) DEFAULT NULL,
  `CgePortfolio` bigint DEFAULT NULL,
  `ValorCotacao` decimal(20,6) DEFAULT NULL,
  `NmClassificacao` varchar(150) DEFAULT NULL,
  `qtyposicao` decimal(20,6) DEFAULT NULL,
  `IdClassificacao` decimal(10,2) DEFAULT NULL,
  `valorfinanceiro` decimal(20,6) DEFAULT NULL,
  `CodAtivo` bigint DEFAULT NULL,
  `NuIsin` varchar(20) DEFAULT NULL,
  `CodTipoAtivo` int DEFAULT NULL,
  `dt_carteira` date DEFAULT NULL,
  `dt_insercao` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `ValorSaldoAtivoSwap` decimal(18,6) DEFAULT NULL,
  `ValorSaldoPassivoSwap` decimal(18,6) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE `TB_ENQ_RISCO_CAPITAL_COBRANCA` (
  `id` int NOT NULL AUTO_INCREMENT,
  `nome_gestor` varchar(200) NOT NULL,
  `email_to` varchar(300) NOT NULL,
  `email_cc` varchar(500) DEFAULT NULL,
  `qtde_fundos` int NOT NULL,
  `data_cobranca` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `status_envio` varchar(20) NOT NULL,
  `tipo_cobranca` varchar(100) DEFAULT NULL,
  `hash_registro` varchar(200) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_hash` (`hash_registro`)
) ENGINE=InnoDB AUTO_INCREMENT=39 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE `TB_ENQ_VALIDACAO_MARGEM` (
  `cge` bigint DEFAULT NULL,
  `fundos` text,
  `gestor` text,
  `validado` text,
  `data` datetime DEFAULT NULL,
  `status_valid` bigint DEFAULT NULL,
  `dt_carga` datetime DEFAULT ((now() - interval 1 day))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

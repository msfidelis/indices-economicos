package inflacao

import (
	"crawlers/pkg/logger"
	"crawlers/pkg/upload"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gocarina/gocsv"
)

type Data struct {
	Referencia             string  `json:"referencia" csv:"referencia"`
	Ano                    string  `json:"ano" csv:"ano"`
	Mes                    string  `json:"mes" csv:"mes"`
	AnoMes                 int64   `json:"ano_mes" csv:"ano_mes"`
	IPCAVariacao           float64 `json:"ipca_variacao" csv:"ipca_variacao"`
	IPCAAcumuladoAno       float64 `json:"ipca_acumulado_ano" csv:"ipca_acumulado_ano"`
	IPCAAcumulado12Meses   float64 `json:"ipca_acumulado_doze_meses" csv:"ipca_acumulado_doze_meses"`
	IPCA15Variacao         float64 `json:"ipca15_variacao" csv:"ipca15_variacao"`
	IPCA15AcumuladoAno     float64 `json:"ipca15_acumulado_ano" csv:"ipca15_acumulado_ano"`
	IPCA15Acumulado12Meses float64 `json:"ipca15_acumulado_doze_meses" csv:"ipca15_acumulado_doze_meses"`
	INPCVariacao           float64 `json:"inpc_variacao" csv:"inpc_variacao"`
	INPCAcumuladoAno       float64 `json:"inpc_acumulado_ano" csv:"inpc_acumulado_ano"`
	INPCAcumulado12Meses   float64 `json:"inpc_acumulado_doze_meses" csv:"inpc_acumulado_doze_meses"`
	IPAVariacao            float64 `json:"ipa_variacao" csv:"ipa_variacao"`
	IPAAcumuladoAno        float64 `json:"ipa_acumulado_ano" csv:"ipa_acumulado_ano"`
	IPCVariacao            float64 `json:"ipc_fipe_variacao" csv:"ipc_fipe_variacao"`
	IPCAcumuladoAno        float64 `json:"ipc_fipe_acumulado_ano" csv:"ipc_fipe_acumulado_ano"`
	INCCVariacao           float64 `json:"incc_variacao" csv:"incc_variacao"`
	INCCAcumuladoAno       float64 `json:"incc_acumulado_ano" csv:"incc_acumulado_ano"`
	SalarioMinimo          float64 `json:"salario_minimo" csv:"salario_minimo"`
	ConsolidacaoAno        bool    `json:"consolidado_ano" csv:"consolidado_ano"`
}

type Inflacao struct {
	Atualizacao   time.Time `json:"data_atualizacao"`
	UnidadeMedida string    `json:"unidade_medida"`
	Fonte         string    `json:"fonte"`
	Data          []Data    `json:"data"`
}

func RunnerConsolidacao() {
	l := logger.Instance()

	runnerName := "Inflação - Consolidacao"

	unidadeMedida := "%"
	fonte := "https://servicodados.ibge.gov.br"

	ipcaFile := "./data/inflacao/ipca.json"
	ipca15File := "./data/inflacao/ipca15.json"
	inpcFile := "./data/inflacao/inpc.json"
	ipaFile := "./data/inflacao/ipa.json"
	ipcFile := "./data/inflacao/ipc-fipe.json"
	inccFile := "./data/inflacao/incc.json"
	salarioMinimoFile := "./data/inflacao/salario_minimo.json"

	file_path := "./data/inflacao/inflacao.json"
	fileNameOutputCSV := "./data/inflacao/inflacao.csv"

	s3KeyCSV := "inflacao/inflacao.csv"
	s3KeyJSON := "inflacao/inflacao.json"

	consolidado := make(map[string]Data)

	inflacao := Inflacao{}

	now := time.Now()
	inflacao.Atualizacao = now
	inflacao.Fonte = fonte
	inflacao.UnidadeMedida = unidadeMedida

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner Consolidar os Datasets")

	IPCA := IPCA{}
	fileIPCA, err := ioutil.ReadFile(ipcaFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", ipcaFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileIPCA), &IPCA)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", ipcaFile).
			Msg("converter para struct")
	}

	IPCA15 := IPCA15{}
	fileIPCA15, err := ioutil.ReadFile(ipca15File)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", ipca15File).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileIPCA15), &IPCA15)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", ipca15File).
			Msg("converter para struct")
	}

	INPC := INPC{}
	fileINPC, err := ioutil.ReadFile(inpcFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", inpcFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileINPC), &INPC)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", inpcFile).
			Msg("converter para struct")
	}

	IPA := IPA{}
	fileIPA, err := ioutil.ReadFile(ipaFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", ipaFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileIPA), &IPA)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", ipaFile).
			Msg("converter para struct")
	}

	IPC := IPC{}
	fileIPC, err := ioutil.ReadFile(ipcFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", ipcFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileIPC), &IPC)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", ipcFile).
			Msg("converter para struct")
	}

	INCC := INCC{}
	fileINCC, err := ioutil.ReadFile(inccFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", inccFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileINCC), &INCC)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", inccFile).
			Msg("converter para struct")
	}

	salarioMinimo := SalarioMinimo{}
	fileSalarioMinimo, err := ioutil.ReadFile(salarioMinimoFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", salarioMinimoFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileSalarioMinimo), &salarioMinimo)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", salarioMinimoFile).
			Msg("converter para struct")
	}

	// Construção do map de referencias
	l.Info().
		Str("Runner", runnerName).
		Msg("Construção do map de referencias")

	for _, ip := range IPCA.Data {

		anoMesStr := fmt.Sprintf("%s%s", ip.Ano, ip.Mes)
		anoMes, _ := strconv.ParseInt(strings.TrimSpace(anoMesStr), 10, 64)

		item := Data{}
		item.Referencia = ip.Referencia
		item.Ano = ip.Ano
		item.Mes = ip.Mes
		item.AnoMes = anoMes

		if ip.Mes == "12" {
			item.ConsolidacaoAno = true
		} else {
			item.ConsolidacaoAno = false
		}

		consolidado[ip.Referencia] = item
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Agregando os Items de IPCA ao Consolidado")

	for _, ip := range IPCA.Data {

		item := consolidado[ip.Referencia]
		item.IPCAVariacao = ip.Variacao
		item.IPCAAcumuladoAno = ip.AcumuladoAno
		item.IPCAAcumulado12Meses = ip.Acumulado12Meses

		consolidado[ip.Referencia] = item
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Agregando os Items de IPCA15 ao Consolidado")

	for _, ip := range IPCA15.Data {

		item := consolidado[ip.Referencia]
		item.IPCA15Variacao = ip.Variacao
		item.IPCA15AcumuladoAno = ip.AcumuladoAno
		item.IPCA15Acumulado12Meses = ip.Acumulado12Meses

		consolidado[ip.Referencia] = item
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Agregando os Items de INPC ao Consolidado")

	for _, in := range INPC.Data {

		item := consolidado[in.Referencia]
		item.INPCVariacao = in.Variacao
		item.INPCAcumuladoAno = in.AcumuladoAno
		item.INPCAcumulado12Meses = in.Acumulado12Meses

		consolidado[in.Referencia] = item
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Agregando os Items de IPA ao Consolidado")

	for _, in := range IPA.Data {

		item := consolidado[in.Referencia]
		item.IPAVariacao = in.Variacao
		item.IPAAcumuladoAno = in.AcumuladoAno

		consolidado[in.Referencia] = item
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Agregando os Items de IPC-FIPE ao Consolidado")

	for _, in := range IPC.Data {

		item := consolidado[in.Referencia]
		item.IPCVariacao = in.Variacao
		item.IPCAcumuladoAno = in.AcumuladoAno

		consolidado[in.Referencia] = item
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Agregando os Items de INCC ao Consolidado")

	for _, in := range INCC.Data {

		item := consolidado[in.Referencia]
		item.INCCVariacao = in.Variacao
		item.INCCAcumuladoAno = in.AcumuladoAno

		consolidado[in.Referencia] = item
	}

	for _, in := range salarioMinimo.Data {

		item := consolidado[in.Referencia]
		item.SalarioMinimo = in.Valor

		consolidado[in.Referencia] = item
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Criando o Struct Final de Inflação")

	for _, i := range consolidado {
		if i.Referencia != "" {
			inflacao.Data = append(inflacao.Data, i)
		}
	}

	// Sort do data
	sort.Slice(inflacao.Data, func(i, j int) bool {
		return inflacao.Data[i].Referencia < inflacao.Data[j].Referencia
	})

	l.Info().
		Str("Runner", runnerName).
		Msg("Convertendo a Struct do Schema em formato JSON")

	b, err := json.Marshal(inflacao)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Msg("Erro ao converter a struct em JSON")
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", file_path).
		Msg("Criando arquivo de persistência para os dados convertidos")

	f, err := os.Create(file_path)
	defer f.Close()

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", file_path).
			Str("Error", err.Error()).
			Msg("Erro ao criar o diretório para persistência dos dados")
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", file_path).
		Msg("Iniciando a escrita dos dados no arquivo de persistência")

	_, err = f.WriteString(string(b))

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", file_path).
			Str("Error", err.Error()).
			Msg("Erro para escrever os dados no arquivo")
	}

	// Convertendo em CSV
	csvFile, err := os.OpenFile(fileNameOutputCSV, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", fileNameOutputCSV).
			Str("Error", err.Error()).
			Msg("Erro ao criar o dataset em CSV")
	}
	defer csvFile.Close()

	csvOutput, err := gocsv.MarshalString(&inflacao.Data)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", fileNameOutputCSV).
			Str("Error", err.Error()).
			Msg("Erro ao escrever o dataset em CSV")
	}

	_, err = csvFile.WriteString(string(csvOutput))
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", fileNameOutputCSV).
			Str("Error", err.Error()).
			Msg("Erro para escrever os dados no arquivo")
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameOutputCSV).
		Msg("Dataset em CSV Criado")

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameOutputCSV).
		Str("S3Key", s3KeyCSV).
		Msg("Fazendo Upload para o S3")

	err = upload.S3(fileNameOutputCSV, s3KeyCSV)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", fileNameOutputCSV).
			Str("S3Key", s3KeyCSV).
			Str("Error", err.Error()).
			Msg("Erro ao fazer upload do arquivo para o S3")
	}

	err = upload.S3(file_path, s3KeyJSON)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", file_path).
			Str("S3Key", s3KeyJSON).
			Str("Error", err.Error()).
			Msg("Erro ao fazer upload do arquivo para o S3")
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameOutputCSV).
		Msg("Finalizado")

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", file_path).
		Msg("Finalizado")

}

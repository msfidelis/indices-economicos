package precos

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
	Referencia string `json:"referencia" csv:"referencia"`
	Ano        string `json:"ano" csv:"ano"`
	Mes        string `json:"mes" csv:"mes"`
	AnoMes     int64  `json:"ano_mes" csv:"ano_mes"`

	CanaDeAcucar           float64 `json:"cana_de_acucar_preco_tonelada" csv:"cana_de_acucar_preco_tonelada"`
	CanaDeAcucarReferencia string  `json:"cana_de_acucar_referencia" csv:"cana_de_acucar_referencia"`

	ArrozTipo1      float64 `json:"arroz_tipo_1_30kg" csv:"arroz_tipo_1_30kg"`
	ArrozTipo2      float64 `json:"arroz_tipo_2_30kg" csv:"arroz_tipo_2_30kg"`
	ArrozReferencia string  `json:"arroz_referencia" csv:"arroz_referencia"`

	Cafe           float64 `json:"cafe_em_coco_preco_kg" csv:"cafe_em_coco_preco_kg"`
	CafeReferencia string  `json:"cafe_referencia" csv:"cafe_referencia"`

	Feijao           float64 `json:"feijao_preco_30kg" csv:"feijao_preco_30kg"`
	FeijaoReferencia string  `json:"feijao_referencia" csv:"feijao_referencia"`

	Milho           float64 `json:"milho_preco_60kg" csv:"milho_preco_60kg"`
	MilhoReferencia string  `json:"milho_referencia" csv:"milho_referencia"`

	Soja           float64 `json:"soja_farelo_preco_tonelada" csv:"soja_farelo_preco_tonelada"`
	SojaReferencia string  `json:"soja_farelo_referencia" csv:"soja_farelo_referencia"`

	ConsolidacaoAno bool `json:"consolidado_ano" csv:"consolidado_ano"`
}

type Precos struct {
	Atualizacao   time.Time `json:"data_atualizacao"`
	UnidadeMedida string    `json:"unidade_medida"`
	Fonte         string    `json:"fonte"`
	Data          []Data    `json:"data"`
}

func RunnerConsolidacao() {
	runnerName := "Preços - Consolidacao"
	fonte := "www.ipeadata.gov.br"
	l := logger.Instance()

	// Files Output
	file_path := "./data/precos/precos.json"
	fileNameOutputCSV := "./data/precos/precos.csv"

	s3KeyCSV := "precos/precos.csv"
	s3KeyJSON := "precos/precos.json"

	// Files dos Precos
	canaFile := "./data/precos/cana.json"
	arrozTipo1File := "./data/precos/arroztipo1-30kg.json"
	arrozTipo2File := "./data/precos/arroztipo2-30kg.json"
	cafeFile := "./data/precos/cafe.json"
	feijaoFile := "./data/precos/cafe.json"
	milhoFile := "./data/precos/milho-60kg.json"
	sojaFile := "./data/precos/soja-tonelada.json"

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	consolidado := make(map[string]Data)

	precos := Precos{}

	now := time.Now()
	precos.Atualizacao = now
	precos.Fonte = fonte

	// Cana
	cana := Cana{}
	fileCana, err := ioutil.ReadFile(canaFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", canaFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileCana), &cana)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", canaFile).
			Msg("converter para struct")
	}

	// Arroz Tipo 1
	arroztipo1 := ArrozTipo130Kg{}
	fileArrozTipo1, err := ioutil.ReadFile(arrozTipo1File)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", arrozTipo1File).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileArrozTipo1), &arroztipo1)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", arrozTipo1File).
			Msg("converter para struct")
	}

	// Arroz Tipo 2
	arroztipo2 := ArrozTipo230Kg{}
	fileArrozTipo2, err := ioutil.ReadFile(arrozTipo2File)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", arrozTipo2File).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileArrozTipo2), &arroztipo2)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", arrozTipo2File).
			Msg("converter para struct")
	}

	// Cafe
	cafe := Cafe{}
	fileCafe, err := ioutil.ReadFile(cafeFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", cafeFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileCafe), &cafe)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", cafeFile).
			Msg("converter para struct")
	}

	// Feijao
	feijao := Feijao30Kg{}
	fileFeijao, err := ioutil.ReadFile(feijaoFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", feijaoFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileFeijao), &feijao)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", feijaoFile).
			Msg("converter para struct")
	}

	// Milho
	milho := Milho60Kg{}
	fileMilho, err := ioutil.ReadFile(milhoFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", milhoFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileMilho), &milho)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", milhoFile).
			Msg("converter para struct")
	}

	// Soja Farelo
	soja := SojaTonelada{}
	fileSoja, err := ioutil.ReadFile(sojaFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", sojaFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileSoja), &soja)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", sojaFile).
			Msg("converter para struct")
	}

	// Construção do map de referencias
	l.Info().
		Str("Runner", runnerName).
		Msg("Construção do map de referencias")

	for _, ip := range cana.Data {

		splitData := strings.Split(ip.Referencia, "-")

		anoMesStr := fmt.Sprintf("%s%s", splitData[0], splitData[1])
		anoMes, _ := strconv.ParseInt(strings.TrimSpace(anoMesStr), 10, 64)

		ano := splitData[0]
		mes := splitData[1]

		item := Data{}
		item.Referencia = ip.Referencia
		item.Ano = ano
		item.Mes = mes
		item.AnoMes = anoMes

		if mes == "12" {
			item.ConsolidacaoAno = true
		} else {
			item.ConsolidacaoAno = false
		}

		consolidado[ip.Referencia] = item
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Criando o Struct Final de Preços")

	//Cana de Açucar
	for _, ip := range cana.Data {
		item := consolidado[ip.Referencia]
		item.CanaDeAcucar = ip.Valor
		item.CanaDeAcucarReferencia = "produtor"

		consolidado[ip.Referencia] = item
	}

	//Cafe
	for _, ip := range cafe.Data {
		item := consolidado[ip.Referencia]
		item.Cafe = ip.Valor
		item.CafeReferencia = "produtor"

		consolidado[ip.Referencia] = item
	}

	//Arroz Tipo 1
	for _, ip := range arroztipo1.Data {
		item := consolidado[ip.Referencia]
		item.ArrozTipo1 = ip.Valor
		item.ArrozReferencia = "atacado"
		consolidado[ip.Referencia] = item
	}

	//Arroz Tipo 2
	for _, ip := range arroztipo2.Data {
		item := consolidado[ip.Referencia]
		item.ArrozTipo2 = ip.Valor
		item.ArrozReferencia = "atacado"
		consolidado[ip.Referencia] = item
	}

	//Feijao
	for _, ip := range feijao.Data {
		item := consolidado[ip.Referencia]
		item.Feijao = ip.Valor
		item.FeijaoReferencia = "atacado"
		consolidado[ip.Referencia] = item
	}

	//Milho
	for _, ip := range milho.Data {
		item := consolidado[ip.Referencia]
		item.Milho = ip.Valor
		item.MilhoReferencia = "atacado"
		consolidado[ip.Referencia] = item
	}

	//Soja Farelo
	for _, ip := range soja.Data {
		item := consolidado[ip.Referencia]
		item.Soja = ip.Valor
		item.SojaReferencia = "atacado"
		consolidado[ip.Referencia] = item
	}

	for _, i := range consolidado {
		if i.Referencia != "" {
			precos.Data = append(precos.Data, i)
		}
	}

	// Sort do data
	sort.Slice(precos.Data, func(i, j int) bool {
		return precos.Data[i].Referencia < precos.Data[j].Referencia
	})

	l.Info().
		Str("Runner", runnerName).
		Msg("Convertendo a Struct do Schema em formato JSON")

	b, err := json.Marshal(precos)
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

	csvOutput, err := gocsv.MarshalString(&precos.Data)
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

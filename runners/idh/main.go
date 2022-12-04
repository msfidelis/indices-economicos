package idh

import (
	"crawlers/pkg/logger"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/structs"
	"github.com/gocarina/gocsv"
)

var (
	fileNameOutput string
	fileNameRaw    string
	fullURLFile    string
)

type Data struct {
	Ano   string  `json:"ano_referencia"`
	Valor float64 `json:"valor"`
}

type HDI struct {
	Atualizacao time.Time `json:"data_atualizacao"`
	Fonte       string    `json:"fonte"`
	Data        []Data    `json:"data"`
}

type HDICsv struct {
	Pais    string `csv:"Country"`
	HDI1991 string `csv:"hdi_1991"`
	HDI1992 string `csv:"hdi_1992"`
	HDI1993 string `csv:"hdi_1993"`
	HDI1994 string `csv:"hdi_1994"`
	HDI1995 string `csv:"hdi_1995"`
	HDI1996 string `csv:"hdi_1996"`
	HDI1997 string `csv:"hdi_1997"`
	HDI1998 string `csv:"hdi_1998"`
	HDI1999 string `csv:"hdi_1999"`
	HDI2000 string `csv:"hdi_2000"`
	HDI2001 string `csv:"hdi_2001"`
	HDI2002 string `csv:"hdi_2002"`
	HDI2003 string `csv:"hdi_2003"`
	HDI2004 string `csv:"hdi_2004"`
	HDI2005 string `csv:"hdi_2005"`
	HDI2006 string `csv:"hdi_2006"`
	HDI2007 string `csv:"hdi_2007"`
	HDI2008 string `csv:"hdi_2008"`
	HDI2009 string `csv:"hdi_2009"`
	HDI2010 string `csv:"hdi_2010"`
	HDI2011 string `csv:"hdi_2011"`
	HDI2012 string `csv:"hdi_2012"`
	HDI2013 string `csv:"hdi_2013"`
	HDI2014 string `csv:"hdi_2014"`
	HDI2015 string `csv:"hdi_2015"`
	HDI2016 string `csv:"hdi_2016"`
	HDI2017 string `csv:"hdi_2017"`
	HDI2018 string `csv:"hdi_2018"`
	HDI2019 string `csv:"hdi_2019"`
	HDI2020 string `csv:"hdi_2020"`
	HDI2021 string `csv:"hdi_2021"`
	HDI2022 string `csv:"hdi_2022"`
	HDI2023 string `csv:"hdi_2023"`
}

func Runner() {
	runnerName := "IDH"

	fullURLFile := "https://hdr.undp.org/modules/custom/hdro_app/static/media/Onlinemaster_HDR2122_081522.ac8500f84b9d9d251f41.csv"
	fileNameRaw := "./data/idh/raw/hdr-raw.csv"
	fileNameOutput := "./data/idh/idh.json"
	fonte := "https://hdr.undp.org/"

	l := logger.Instance()
	now := time.Now()
	hdi := &HDI{}
	hdi.Fonte = fonte
	hdi.Atualizacao = now

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameRaw).
		Msg("Criando o arquivo de recepção temporario")

	f, err := os.Create(fileNameRaw)
	defer f.Close()

	if err != nil {
		l.Error().
			Str("Runner", runnerName).
			Str("FilePath", fileNameRaw).
			Str("Erro", err.Error()).
			Msg("Falha ao criar o arquivo temporario")
	}

	client := http.Client{
		CheckRedirect: func(r *http.Request, via []*http.Request) error {
			r.URL.Opaque = r.URL.Path
			return nil
		},
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameOutput).
		Str("URL", fullURLFile).
		Msg("Realizando o download do arquivo")

	resp, err := client.Get(fullURLFile)
	if err != nil {
		l.Error().
			Str("Runner", runnerName).
			Str("FilePath", fileNameOutput).
			Str("URL", fullURLFile).
			Str("Erro", err.Error()).
			Msg("Erro ao fazer o request HTTP para a URL selecionada")
	}
	defer resp.Body.Close()

	size, err := io.Copy(f, resp.Body)
	if err != nil {
		l.Error().
			Str("Runner", runnerName).
			Str("FilePath", fileNameRaw).
			Str("URL", fullURLFile).
			Str("Erro", err.Error()).
			Msg("Erro ao escrever no arquivo temporario")
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameRaw).
		Str("URL", fullURLFile).
		Int64("Size", size).
		Msg("Escrita no arquivo temporário concluído")

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameRaw).
		Msg("Lendo o arquivo temporário")

	tmpFile, err := os.OpenFile(fileNameRaw, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		panic(err)
	}
	defer tmpFile.Close()

	hdiCsv := []*HDICsv{}

	if err := gocsv.UnmarshalFile(tmpFile, &hdiCsv); err != nil {
		l.Error().
			Str("Runner", runnerName).
			Str("FilePath", fileNameRaw).
			Str("URL", fullURLFile).
			Str("Erro", err.Error()).
			Msg("Erro ao converter o CSV em Struct")
		panic(err)
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameRaw).
		Msg("Recuperando os dados brasileiros")

	for _, pais := range hdiCsv {

		if pais.Pais == "Brazil" {

			// Recupera o nome dos campos da Struct onde ficam os valores do PIB
			campos := structs.Names(pais)

			for _, v := range campos[1:] {

				r := reflect.ValueOf(pais)
				f := reflect.Indirect(r).FieldByName(v)

				ano := v[3:7]

				valorStr := fmt.Sprintf("0%v", f.String())
				valor, err := strconv.ParseFloat(strings.TrimSpace(valorStr), 64)

				if err != nil {
					l.Fatal().
						Str("Runner", runnerName).
						Str("Error", err.Error()).
						Str("Valor recuperado", valorStr).
						Msg("Erro ao converter o valor para Float64")
				}

				item := &Data{
					Ano:   ano,
					Valor: valor,
				}

				if item.Valor > 0 {
					hdi.Data = append(hdi.Data, *item)
				}

			}

		}

	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameOutput).
		Msg("Criando o arquivo final")

	o, err := os.Create(fileNameOutput)
	defer o.Close()

	if err != nil {
		l.Error().
			Str("Runner", runnerName).
			Str("FilePath", fileNameOutput).
			Str("Erro", err.Error()).
			Msg("Falha ao criar o arquivo final")
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameOutput).
		Msg("Arquivo criado")

	l.Info().
		Str("Runner", runnerName).
		Msg("Convertendo a Struct do Schema em formato JSON")

	b, err := json.Marshal(hdi)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Msg("Erro ao converter a struct em JSON")
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameOutput).
		Msg("Iniciando a escrita dos dados no arquivo de persistência")

	_, err = o.WriteString(string(b))

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", fileNameOutput).
			Str("Error", err.Error()).
			Msg("Erro para escrever os dados no arquivo")
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameRaw).
		Msg("Removendo arquivo temporario")

	err = os.Remove(fileNameRaw)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", fileNameOutput).
			Str("Error", err.Error()).
			Msg("Erro para escrever os dados no arquivo")
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameOutput).
		Msg("Finalizado")

}

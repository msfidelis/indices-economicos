package idh

import (
	"crawlers/pkg/logger"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"sort"
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
	Ano               string  `json:"ano_referencia"`
	IDH               float64 `json:"idh"`
	IDHF              float64 `json:"idh_feminino"`
	IDHM              float64 `json:"idh_masculino"`
	ExpectativaDeVida float64 `json:"expectativa_de_vida"`
}

type HDI struct {
	Atualizacao time.Time `json:"data_atualizacao"`
	Fonte       string    `json:"fonte"`
	Data        []Data    `json:"data"`
}

type HDICsv struct {
	Pais string `csv:"Country"`

	//HDI
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

	// IDH Feminino
	FHD1991 string `csv:"hdi_f_1991"`
	FHD1992 string `csv:"hdi_f_1992"`
	FHD1993 string `csv:"hdi_f_1993"`
	FHD1994 string `csv:"hdi_f_1994"`
	FHD1995 string `csv:"hdi_f_1995"`
	FHD1996 string `csv:"hdi_f_1996"`
	FHD1997 string `csv:"hdi_f_1997"`
	FHD1998 string `csv:"hdi_f_1998"`
	FHD1999 string `csv:"hdi_f_1999"`
	FHD2000 string `csv:"hdi_f_2000"`
	FHD2001 string `csv:"hdi_f_2001"`
	FHD2002 string `csv:"hdi_f_2002"`
	FHD2003 string `csv:"hdi_f_2003"`
	FHD2004 string `csv:"hdi_f_2004"`
	FHD2005 string `csv:"hdi_f_2005"`
	FHD2006 string `csv:"hdi_f_2006"`
	FHD2007 string `csv:"hdi_f_2007"`
	FHD2008 string `csv:"hdi_f_2008"`
	FHD2009 string `csv:"hdi_f_2009"`
	FHD2010 string `csv:"hdi_f_2010"`
	FHD2011 string `csv:"hdi_f_2011"`
	FHD2012 string `csv:"hdi_f_2012"`
	FHD2013 string `csv:"hdi_f_2013"`
	FHD2014 string `csv:"hdi_f_2014"`
	FHD2015 string `csv:"hdi_f_2015"`
	FHD2016 string `csv:"hdi_f_2016"`
	FHD2017 string `csv:"hdi_f_2017"`
	FHD2018 string `csv:"hdi_f_2018"`
	FHD2019 string `csv:"hdi_f_2019"`
	FHD2020 string `csv:"hdi_f_2020"`
	FHD2021 string `csv:"hdi_f_2021"`
	FHD2022 string `csv:"hdi_f_2022"`
	FHD2023 string `csv:"hdi_f_2023"`

	// IDH Masculino
	MHD1991 string `csv:"hdi_m_1991"`
	MHD1992 string `csv:"hdi_m_1992"`
	MHD1993 string `csv:"hdi_m_1993"`
	MHD1994 string `csv:"hdi_m_1994"`
	MHD1995 string `csv:"hdi_m_1995"`
	MHD1996 string `csv:"hdi_m_1996"`
	MHD1997 string `csv:"hdi_m_1997"`
	MHD1998 string `csv:"hdi_m_1998"`
	MHD1999 string `csv:"hdi_m_1999"`
	MHD2000 string `csv:"hdi_m_2000"`
	MHD2001 string `csv:"hdi_m_2001"`
	MHD2002 string `csv:"hdi_m_2002"`
	MHD2003 string `csv:"hdi_m_2003"`
	MHD2004 string `csv:"hdi_m_2004"`
	MHD2005 string `csv:"hdi_m_2005"`
	MHD2006 string `csv:"hdi_m_2006"`
	MHD2007 string `csv:"hdi_m_2007"`
	MHD2008 string `csv:"hdi_m_2008"`
	MHD2009 string `csv:"hdi_m_2009"`
	MHD2010 string `csv:"hdi_m_2010"`
	MHD2011 string `csv:"hdi_m_2011"`
	MHD2012 string `csv:"hdi_m_2012"`
	MHD2013 string `csv:"hdi_m_2013"`
	MHD2014 string `csv:"hdi_m_2014"`
	MHD2015 string `csv:"hdi_m_2015"`
	MHD2016 string `csv:"hdi_m_2016"`
	MHD2017 string `csv:"hdi_m_2017"`
	MHD2018 string `csv:"hdi_m_2018"`
	MHD2019 string `csv:"hdi_m_2019"`
	MHD2020 string `csv:"hdi_m_2020"`
	MHD2021 string `csv:"hdi_m_2021"`
	MHD2022 string `csv:"hdi_m_2022"`
	MHD2023 string `csv:"hdi_m_2023"`
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

	ordenado := make(map[string]Data)

	for _, pais := range hdiCsv {

		if pais.Pais == "Brazil" {

			// Recupera o nome dos campos da Struct
			campos := structs.Names(pais)

			// Construindo o Campo do IDH Geral
			for _, v := range campos[1:] {

				if strings.HasPrefix(v, "HDI") {

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
						Ano: ano,
						IDH: valor,
					}

					if item.IDH > 0 {
						ordenado[ano] = *item
					}

				}

			}

			// Construindo o Campo do IDH Feminino
			for _, v := range campos[1:] {

				if strings.HasPrefix(v, "FHD") {

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

					item := ordenado[ano]
					item.IDHF = valor

					ordenado[ano] = item

				}

			}

			// Construindo o Campo do IDH Masculino
			for _, v := range campos[1:] {

				if strings.HasPrefix(v, "MHD") {

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

					item := ordenado[ano]
					item.IDHM = valor

					ordenado[ano] = item

				}

			}

		}

	}

	for _, i := range ordenado {
		hdi.Data = append(hdi.Data, i)
	}

	sort.Slice(hdi.Data, func(i, j int) bool {
		return hdi.Data[i].Ano < hdi.Data[j].Ano
	})

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

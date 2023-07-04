package main

import (
	"crawlers/pkg/logger"
	"crawlers/runners/ambientais"
	"crawlers/runners/gini"
	"crawlers/runners/idh"
	"crawlers/runners/igpm"
	"crawlers/runners/inflacao"
	"crawlers/runners/inpc"
	"crawlers/runners/ipca"
	"crawlers/runners/pib"
	"crawlers/runners/precos"
	"crawlers/runners/selic"
	"crawlers/runners/sociais"
	"os"
	"sync"
)

func main() {

	var wg sync.WaitGroup
	l := logger.Instance()

	batch_index := os.Getenv("AWS_BATCH_JOB_ARRAY_INDEX")

	if batch_index == "0" {

		l.Warn().
			Msg("Skipping Index")

		os.Exit(0)
	}

	l.Info().
		Msg("Iniciando o processo de Crawling dos dados abertos")

	if batch_index == "1" || batch_index == "" {

		// Preços
		l.Info().
			Msg("Iniciando o Runner de Preços")

		wg.Add(1)
		go func() {
			defer wg.Done()
			precos.Runner()
		}()

		// Inflação
		l.Info().
			Msg("Iniciando o Runner de Inflação")

		wg.Add(1)
		go func() {
			defer wg.Done()
			inflacao.Runner()
		}()

		// IDH
		l.Info().
			Msg("Iniciando o Runner de coeficiente de IDH")

		wg.Add(1)
		go func() {
			defer wg.Done()
			idh.Runner()
		}()

		// PIB
		l.Info().
			Msg("Iniciando o Runner de PIB")

		wg.Add(1)
		go func() {
			defer wg.Done()
			pib.Runner()
		}()

		// IPCA
		l.Info().
			Msg("Iniciando o Runner de IPCA")

		wg.Add(1)
		go func() {
			defer wg.Done()
			ipca.Runner()
		}()

		// INPC
		l.Info().
			Msg("Iniciando o Runner de INPC")

		wg.Add(1)
		go func() {
			defer wg.Done()
			inpc.Runner()
		}()

		// IGP-M
		l.Info().
			Msg("Iniciando o Runner de IGP-M")

		wg.Add(1)
		go func() {
			defer wg.Done()
			igpm.Runner()
		}()

		// SELIC
		l.Info().
			Msg("Iniciando o Runner de Selic")

		wg.Add(1)
		go func() {
			defer wg.Done()
			selic.Runner()
		}()

		// GINI
		l.Info().
			Msg("Iniciando o Runner de coeficiente de Gini")

		wg.Add(1)
		go func() {
			defer wg.Done()
			gini.Runner()
		}()

		// Sociais
		l.Info().
			Msg("Iniciando o Runner de Indicadores Sociais")

		wg.Add(1)
		go func() {
			defer wg.Done()
			sociais.Runner()
		}()

		// Ambientais
		l.Info().
			Msg("Iniciando o Runner de Indicadores Ambientais")

		wg.Add(1)
		go func() {
			defer wg.Done()
			ambientais.Runner()
		}()

	}

	wg.Wait()

}

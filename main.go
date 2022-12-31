package main

import (
	"crawlers/pkg/logger"
	"crawlers/runners/gini"
	"crawlers/runners/idh"
	"crawlers/runners/igpm"
	"crawlers/runners/inflacao"
	"crawlers/runners/inpc"
	"crawlers/runners/ipca"
	"crawlers/runners/pib"
	"crawlers/runners/selic"
	"crawlers/runners/sociais"
	"sync"
)

func main() {
	var wg sync.WaitGroup
	l := logger.Instance()
	l.Info().
		Msg("Iniciando o processo de Crawling dos dados abertos")

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

	pib.Runner()

	// IPCA
	l.Info().
		Msg("Iniciando o Runner de IPCA")

	ipca.Runner()

	// INPC
	l.Info().
		Msg("Iniciando o Runner de INPC")

	inpc.Runner()

	// IGP-M
	l.Info().
		Msg("Iniciando o Runner de IGP-M")

	igpm.Runner()

	// SELIC
	l.Info().
		Msg("Iniciando o Runner de Selic")

	selic.Runner()

	// GINI
	l.Info().
		Msg("Iniciando o Runner de coeficiente de Gini")
	gini.Runner()

	// Sociais
	l.Info().
		Msg("Iniciando o Runner de Indicadores Sociais")
	sociais.Runner()

}

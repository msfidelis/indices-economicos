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

	// wg.Add(1)
	// go func() {
	// 	defer wg.Done()
	// 	selic.Runner()
	// }()

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

	wg.Wait()

}

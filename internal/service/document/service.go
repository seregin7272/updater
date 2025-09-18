package document

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"updater/internal/entity/document"
)

// На вход сервису поступают обновления документов
//
// message Document {
//   Url            text  // URL документа, его уникальный идентификатор
//   PubDate        uint64  // время заявляемой публикации документа
//   FetchTime      uint64  // время получения данного обновления документа, может рассматриваться как идентификатор версии. Пара (Url, FetchTime) уникальна.
//   Text           text  // текст документа
//   FirstFetchTime *uint64 // изначально отсутствует, необходимо заполнить
// }
//
// Документы могут поступать в произвольном порядке (не в том, как они обновлялись), также возможно
//    дублирование отдельных сообщений.
//
// Необходимо на выходе формировать такие же сообщения, но с исправленными отдельными полями по
//    следующим правилам (всё нижеуказанное - для группы документов с совпадающим полем Url):
//
// Поле Text (Text) и FetchTime (FetchTime) должны быть такими, какими были в документе с наибольшим FetchTime, полученным на данный момент
// Поле PubDate (PubDate) должно быть таким, каким было у сообщения с наименьшим FetchTime
// Поле FirstFetchTime (FetchTime) должно быть равно минимальному значению FetchTime

// Т.е. в каждый момент времени мы берём PubDate и FirstFetchTime от самой первой из полученных
//    на данный момент версий (если отсортировать их по FetchTime), а Text и FetchTime - от самой последней.
//
// Интерфейс в коде можно реализовать таким:
//
// type Processor interface {
//   Process(doc *Document) (*Document, error)
// }
//
// Данный код будет работать в сервисе, читающим входные сообщения из очереди сообщений (Kafka или подобное),
// и записывающем результат также в очередь. Если Process возвращает Null - то в очередь ничего не пишется.

// message Document {
//   Url            text  // URL документа, его уникальный идентификатор
//   PubDate        uint64  // время заявляемой публикации документа
//   FetchTime      uint64  // время получения данного обновления документа, может рассматриваться как идентификатор версии. Пара (Url, FetchTime) уникальна.
//   Text           text  // текст документа
//   FirstFetchTime *uint64 // изначально отсутствует, необходимо заполнить
// }

//doc1 ( Url, FetchTime)v2 textv2 PubDatev1 FirstFetchTimev1

//doc2 ( Url FetchTime) v1 text PubDate

type Repo interface {
	GetByURL(ctx context.Context, tx pgx.Tx, url string) (*document.Model, error)
	UpsertDoc(ctx context.Context, tx pgx.Tx, doc *document.Model) error
	Tx(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) error
}

type Service struct {
	repo Repo
}

func NewService(repo Repo) *Service {
	return &Service{repo: repo}
}

func (s *Service) UpsertDoc(ctx context.Context, doc *document.Model) (*document.Model, error) {
	var updated *document.Model

	err := s.repo.Tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		d, err := s.repo.GetByURL(ctx, tx, doc.Url)
		if err != nil {
			return fmt.Errorf("repo.GetByURL: %w", err)
		}

		/*
			// Поле Text (Text) и FetchTime (FetchTime) должны быть такими, какими были в документе с наибольшим FetchTime, полученным на данный момент
			// Поле PubDate (PubDate) должно быть таким, каким было у сообщения с наименьшим FetchTime
			// Поле FirstFetchTime (FetchTime) должно быть равно минимальному значению FetchTime

			// Т.е. в каждый момент времени мы берём PubDate и FirstFetchTime от самой первой из полученных
			//    на данный момент версий (если отсортировать их по FetchTime), а Text и FetchTime - от самой последней.
			//
		*/

		updated = d

		if d != nil {
			if doc.FetchTime > d.FetchTime {
				updated.FetchTime = doc.FetchTime
				updated.Text = doc.Text
			}

			if doc.FetchTime < *d.FirstFetchTime {
				updated.PubDate = doc.PubDate
				updated.FirstFetchTime = &doc.FetchTime
			}

		} else {
			updated = doc
			updated.FirstFetchTime = &doc.FetchTime
		}

		err = s.repo.UpsertDoc(ctx, tx, updated)

		if err != nil {
			return fmt.Errorf("repo.UpsertDoc: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("s.repo.Tx: %w", err)
	}

	return updated, nil
}

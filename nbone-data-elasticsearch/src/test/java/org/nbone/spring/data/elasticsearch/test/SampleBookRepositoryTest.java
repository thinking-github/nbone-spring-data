package org.nbone.spring.data.elasticsearch.test;

import com.chainnova.cactus.analytics.domain.TssCommonData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.GetQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.data.elasticsearch.repositories.book.Book;
import org.springframework.data.elasticsearch.repositories.book.SampleBookRepository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:/spring/spring-context.xml")
public class SampleBookRepositoryTest {

	@Resource
	private SampleBookRepository repository;


    @Resource
	private ElasticsearchTemplate template;

	@Before
    public void emptyData(){
       // repository.deleteAll();
    }


	@Test
	public void shouldIndexSingleBookEntity(){

		Book book = new Book();
		book.setId("123455");
		book.setName("Spring Data Elasticsearch");
		book.setVersion(System.currentTimeMillis());
		Book book1 = repository.save(book);
		//lets try to search same record in elasticsearch
		//Book indexedBook = repository.findOne(book.getId());
		//Optional<Book> optional  = repository.findById(book.getId());
		//Book book2 = optional.get();

	}

	@Test
	public void getByIndexId(){

		GetQuery getQuery = new GetQuery();
		getQuery.setId("AWd-EddLxboxWPySyRHO");
		Book book = template.queryForObject(getQuery,Book.class);


	}

	@Test
	public void shouldReturnBooksForGivenBucketUsingTemplate(){

		template.deleteIndex(Book.class);
		boolean  created = template.createIndex(Book.class);
		template.putMapping(Book.class);
		template.refresh(Book.class);
	 	//Map<String,Object> mapping = template.getMapping(Book.class);

		//repository.save(Arrays.asList(book1,book2));



		//Page<Book> books = repository.search(searchQuery);

		//assertThat(books.getContent().size(), is(1));
	}


	@Test
	public void getEntity(){

		GetQuery getQuery = new GetQuery();
		getQuery.setId("AWfBPFSAVxxYoA-d-98-");
		TssCommonData commonData = template.queryForObject(getQuery,TssCommonData.class);
		System.out.println(commonData);



		/*SearchQuery searchQuery = new NativeSearchQueryBuilder()
				.withQuery(matchAllQuery())
				.withFilter(boolFilter().must(termFilter("id", documentId)))
				.build();*/

		SearchQuery searchQuery1 = new NativeSearchQueryBuilder()
				.withQuery(matchAllQuery())
				.withIndices("salix_693fe72810_5f198ab220_23198cd100")
				.withTypes("salix_693fe72810_5f198ab220_23198cd100")
				//.withFields("message")
				.withPageable(PageRequest.of(0, 10))
				.build();

		Page<TssCommonData> sampleEntities = template.queryForPage(searchQuery1,TssCommonData.class);
		System.out.println(sampleEntities);



	}



}

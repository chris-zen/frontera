"""
Frontier backend implementation based on the OPIC algorithm adaptated
to HITS scoring.

OPIC stands for On-Line Page Importance Computation and is described
in:

    Adaptive On-Line Page Importance Computation
    Abiteboul S., Preda M., Cobena G.
    2003

HITS stands for Hyperlink-Induced Topic Search, originally described
in:

    Authoritative sources in a hyperlinked environment
    Kleinber J.
    1998

The basic idea of this backend is to crawl pages with a frequency
proportional to a weighted average of the hub and authority scores.
"""
import datetime

from crawlfrontier import Backend
from crawlfrontier.core.models import Request

from opichits import OpicHits

import graphdb
import scoredb
import pagedb
import hitsdb

class OpicHitsBackend(Backend):    
    """Frontier backend implementation based on the OPIC algorithm adaptated
    to HITS scoring
    """

    component_name = 'OPIC-HITS Backend'

    def __init__(
            self, 
            manager, 
            db_graph=None, 
            db_scores=None, 
            db_pages=None, 
            db_hits=None,
            test=False 
    ):

        # Adjacency between pages and links
        self._graph = db_graph or graphdb.SQLite()
        # Score database to decide next page to crawl
        self._scores = db_scores or scoredb.SQLite()
        # Additional information (URL, domain) 
        self._pages = db_pages or pagedb.SQLite()
        # Implementation of the algorithm
        self._hits = db_hits or hitsdb.SQLite()

        self._opic = OpicHits(
            db_graph=self._graph,
            db_scores=self._hits
        )

        self._test = test

    # FrontierManager interface
    @classmethod
    def from_manager(cls, manager):

        in_memory = manager.settings.get('BACKEND_OPIC_IN_MEMORY', False)
        if not in_memory:
            now = datetime.datetime.utcnow()
            date_str = 'D{0}{1:02d}{2:02d}T{3:02d}{4:02d}{5:02d}'.format(
                now.year,
                now.month,
                now.day,
                now.hour,
                now.minute,
                now.second
            )
            db_graph = graphdb.SQLite(
                manager.settings.get(
                    'BACKEND_OPIC_GRAPH_DB', 
                    'graph-{0}.sqlite'.format(date_str)
                )
            )
            db_pages = pagedb.SQLite(
                manager.settings.get(
                    'BACKEND_OPIC_SCORES_DB', 
                    'pages-{0}.sqlite'.format(date_str)
                )
            )
            db_scores = scoredb.SQLite(
                manager.settings.get(
                    'BACKEND_OPIC_PAGES_DB', 
                    'scores-{0}.sqlite'.format(date_str)
                )
            )
            db_hits = hitsdb.SQLite(
                manager.settings.get(
                    'BACKEND_OPIC_HITS_DB', 
                    'hits-{0}.sqlite'.format(date_str)
                )
            )
        else:
            db_graph = None
            db_pages = None
            db_scores = None
            db_hits = None

        return cls(manager, db_graph, db_scores, db_pages, db_hits,
                   test=manager.settings.get('BACKEND_TEST', False))

    # FrontierManager interface
    def frontier_start(self):
        pass

    # FrontierManager interface
    def frontier_stop(self):
        if self._test:
            self._h_scores = self.h_scores()
            self._a_scores = self.a_scores()
            self._page_scores = self.page_scores()

        self._graph.close()
        self._scores.close()
        self._pages.close()
        self._opic.close()

    # Add pages
    ####################################################################
    def _add_new_link(self, link, score=1.0):
        """Add a new node to the graph, if not present

        Returns the fingerprint used to add the link
        """         
        fingerprint = link.meta['fingerprint']
        self._graph.add_node(fingerprint)
        self._opic.add_new_page(fingerprint)
        self._pages.add(fingerprint,
                        pagedb.PageData(link.url, link.meta['domain']['name']))
        self._scores.add(fingerprint, score)

        return fingerprint

    # FrontierManager interface
    def add_seeds(self, seeds):
        """Start crawling from this seeds"""
        self._graph.start_batch()
        self._pages.start_batch()

        for seed in seeds:
            self._add_new_link(seed)

        self._graph.end_batch()
        self._pages.end_batch()

    # FrontierManager interface
    def page_crawled(self, response, links):
        """Add page info to the graph and reset its score"""
        page_fingerprint = response.meta['fingerprint']
        page_h, page_a = self._opic.get_scores(page_fingerprint)
        self._graph.start_batch()
        for link in links:
            link_fingerprint = self._add_new_link(link, 0.7*page_h + 0.3*page_a)        
            self._graph.add_edge(page_fingerprint, link_fingerprint)
        self._pages.end_batch()

    # FrontierManager interface
    def request_error(self, page, error):
        """TODO"""
        pass

    # Retrieve pages
    ####################################################################
    def _get_request_from_id(self, page_id):
        page_data = self._pages.get(page_id)
        if page_data:
            result = Request(page_data.url)
            result.meta['fingerprint'] = page_id
            if not 'domain' in result.meta:
                result.meta['domain'] = {}
            result.meta['domain']['name'] = page_data.domain
        else:
            result = None

        return result

    # FrontierManager interface
    def get_next_requests(self, max_n_requests):
        """Retrieve the next pages to be crawled"""
        updated = self._opic.update()
        for page_id in updated:
            h_score, a_score = self._opic.get_scores(page_id)
            for link_id in self._graph.neighbours(page_id):
                score = self._scores.get(link_id)
                if score != 0: # otherwise it has already been crawled
                    self._scores.set(link_id, 0.7*h_score + 0.3*a_score)
        
        # build requests for the best scores
        best_scores = self._scores.get_best_scores(max_n_requests)
        requests = filter(
            lambda x: x!= None,
            [self._get_request_from_id(page_id) 
             for page_id, page_data in  best_scores]
        )

        # do not re-crawl
        for page_id, page_data in best_scores:
            self._scores.set(page_id, 0.0)

        return requests


    # Just for testing/debugging
    ####################################################################
    def h_scores(self):
        if self._scores._closed:
            return self._h_scores
        else:
            return {self._pages.get(page_id).url: h_score 
                    for page_id, h_score, a_score in self._opic.iscores()}

    def a_scores(self):
        if self._scores._closed:
            return self._a_scores
        else:
            return {self._pages.get(page_id).url: a_score 
                    for page_id, h_score, a_score in self._opic.iscores()}

    def page_scores(self):
        if self._pages._closed:
            return self._page_scores
        else:
            return {self._pages.get(page_id).url: 0.7*h_score + 0.3*a_score
                    for page_id, h_score, a_score in self._opic.iscores()}

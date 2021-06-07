import os
import pathlib
from urllib.parse import quote_plus

import scrapy
import slugify

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError

BASE_PATH = os.environ.get('BASE_PATH', '/home/mpizarro/devel/freebsd_graphics')
SCRAPE_MODE = os.environ.get('SCRAPE_MODE', 'live')


class NvidiaSpider(scrapy.Spider):
    '''
    scrapy crawl nvidia
    '''
    name = 'nvidia'
    allowed_domains = ['nvidia.com', 'www.nvidia.com', 'web.archive.org']
    custom_settings = {
        'ROBOTSTXT_OBEY': False
    }
    start_urls = [
        'https://www.nvidia.com/en-us/drivers/unix/freebsd-x64-archive/',
        'https://www.nvidia.com/en-us/drivers/unix/freebsd-archive/',
    ] if SCRAPE_MODE=='live' else [
        f'file://{BASE_PATH}/www_data/data/local/https_www_nvidia.com_en-us_drivers_unix_freebsd-archive.html',
        f'file://{BASE_PATH}/www_data/data/local/https_www_nvidia.com_en-us_drivers_unix_freebsd-x64-archive.html',
    ]

    def parse(self, response):
        if SCRAPE_MODE=='live':
            page = response.url.split("/")[-2]
        else:
            page = response.url.split("_")[-1].strip('.html')
        filename = f'{BASE_PATH}/www_data/data/nvidia/driver_lists/nvidia-{page}.html'
        with open(filename, 'wb') as f:
            f.write(response.body)

        default_os = 'FreeBSD x64' if 'x64' in response.url else 'FreeBSD x86'
        for driver in response.css('div.pressItem'):
            spec = {
                'Version': 'unknown',
                'Operating System': default_os,
                'Release Date': 'unknown',
            }
            specs = driver.css('p::text').getall()
            for item in specs:
                if ':' in item:
                    key, value = item.split(':')
                    spec[key.strip()] = value.strip()
            url = driver.css('h4 a::attr(href)').get()
            if url[0:2] == '//':
                url = f'https:{url}'
            version = spec['Version']
            os = spec['Operating System']
            release_date = spec['Release Date']
            filename = f'{BASE_PATH}/www_data/data/nvidia/driver_specs/{os}_nvidia_{version}.html'
            if pathlib.Path(filename).exists():
                yield scrapy.Request(f'file://{filename}', callback=self.parse_driver_specs, cb_kwargs={'filename': filename})
            else:
                yield scrapy.Request(url, callback=self.retrieve_driver_specs, errback=self.handle_errors, meta={'dont_retry': True}, cb_kwargs={'filename': filename})
        # yield scrapy.Request('https://www.nvidia.com/object/freebsd-x86-313.18-driver', callback=self.retrieve_driver_specs, errback=self.handle_errors, cb_kwargs={'filename': filename})
            # yield {
            #     'version': version,
            #     'os': os,
            #     'release_date': release_date,
            #     'url': url,
            # }
        # yield scrapy.Request('https://www.nvidia.com/object/freebsd-x64-331.13-driver', callback=self.parse_driver_specs, cb_kwargs=spec)

    def parse_driver_specs(self, response, filename):
        pass

    def retrieve_driver_specs(self, response, filename):
        with open(filename, 'wb') as f:
            f.write(response.body)

    def retrieve_wayback_machine_capture(self, response, filename, original_url):
        last_ts = response.json()['last_ts']
        url = f'https://web.archive.org/web/{last_ts}/{original_url}'
        yield scrapy.Request(
            url,
            callback=self.retrieve_driver_specs,
            headers={'Referer': 'https://web.archive.org/web/2015*/https://www.nvidia.com/object/frds86-313.18-driver'},
            errback=self.handle_errors,
            meta={'dont_retry': True},
            cb_kwargs={'filename': filename}
        )

    def handle_errors(self, failure):
        if failure.check(HttpError) and failure.value.response.status==503:
            original_url = failure.value.response.url
            url = f'https://web.archive.org/__wb/sparkline?output=json&url={quote_plus(original_url)}&collection=web'
            filename = failure.value.response.cb_kwargs['filename']
            yield scrapy.Request(
                url,
                callback=self.retrieve_wayback_machine_capture,
                headers={'Referer': 'https://web.archive.org/web/2015*/https://www.nvidia.com/object/frds86-313.18-driver'},
                errback=self.handle_errors,
                meta={'dont_retry': True},
                cb_kwargs={'filename': filename, 'original_url': original_url}
            )
        else:
            # log all failures
            self.logger.error(repr(failure))

            if failure.check(HttpError):
                # these exceptions come from HttpError spider middleware
                # you can get the non-200 response
                response = failure.value.response
                self.logger.error('HttpError on %s', response.url)
            elif failure.check(DNSLookupError):
                # this is the original request
                request = failure.request
                self.logger.error('DNSLookupError on %s', request.url)
            elif failure.check(TimeoutError, TCPTimedOutError):
                request = failure.request
                self.logger.error('TimeoutError on %s', request.url)


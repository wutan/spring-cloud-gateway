/*
 * Copyright 2013-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.springframework.cloud.gateway.filter;

import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.ReadTimeoutHandler;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.NettyPipeline;
import reactor.ipc.netty.http.client.HttpClient;
import reactor.ipc.netty.http.client.HttpClientRequest;
import reactor.ipc.netty.http.client.HttpClientResponse;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.cloud.gateway.config.HttpClientProperties;
import org.springframework.cloud.gateway.filter.headers.HttpHeadersFilter;
import org.springframework.cloud.gateway.filter.headers.HttpHeadersFilter.Type;
import org.springframework.cloud.gateway.support.TimeoutException;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.NettyDataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.AbstractServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ServerWebExchange;

import static org.springframework.cloud.gateway.filter.headers.HttpHeadersFilter.filterRequest;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.CLIENT_RESPONSE_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.ORIGINAL_RESPONSE_CONTENT_TYPE_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.PRESERVE_HOST_HEADER_ATTRIBUTE;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.isAlreadyRouted;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.setAlreadyRouted;

/**
 * @author Spencer Gibb
 * @author Biju Kunjummen
 *
 * Netty 路由网关过滤器。其根据 http:// 或 https:// 前缀( Scheme )过滤处理，
 * 使用基于 Netty 实现的 HttpClient 请求后端 Http 服务。
 */

public class NettyRoutingFilter implements GlobalFilter, Ordered {

	//  通过该属性，请求后端的 Http 服务
	private final HttpClient httpClient;

	private final ObjectProvider<List<HttpHeadersFilter>> headersFiltersProvider;
	private final HttpClientProperties properties;

	//do not use this headersFilters directly, use getHeadersFilters() instead.
	private volatile List<HttpHeadersFilter> headersFilters;

	public NettyRoutingFilter(HttpClient httpClient,
							  ObjectProvider<List<HttpHeadersFilter>> headersFiltersProvider,
							  HttpClientProperties properties) {
		this.httpClient = httpClient;
		this.headersFiltersProvider = headersFiltersProvider;
		this.properties = properties;
	}

	public List<HttpHeadersFilter> getHeadersFilters() {
		if (headersFilters == null) {
			headersFilters = headersFiltersProvider.getIfAvailable();
		}
		return headersFilters;
	}

	@Override
	public int getOrder() {
		return Ordered.LOWEST_PRECEDENCE;
	}

	@Override
	public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
		URI requestUrl = exchange.getRequiredAttribute(GATEWAY_REQUEST_URL_ATTR);

		String scheme = requestUrl.getScheme();
		if (isAlreadyRouted(exchange) || (!"http".equals(scheme) && !"https".equals(scheme))) {
			return chain.filter(exchange);
		}


		setAlreadyRouted(exchange);

		ServerHttpRequest request = exchange.getRequest();

		final HttpMethod method = HttpMethod.valueOf(request.getMethodValue());
		final String url = requestUrl.toString();   // 获取URL

		HttpHeaders filtered = filterRequest(getHeadersFilters(), exchange);

		// Request Header
		final DefaultHttpHeaders httpHeaders = new DefaultHttpHeaders();
		filtered.forEach(httpHeaders::set);

		String transferEncoding = request.getHeaders().getFirst(HttpHeaders.TRANSFER_ENCODING);
		boolean chunkedTransfer = "chunked".equalsIgnoreCase(transferEncoding);

		boolean preserveHost = exchange.getAttributeOrDefault(PRESERVE_HOST_HEADER_ATTRIBUTE, false);


		// 请求
		Mono<HttpClientResponse> responseMono = this.httpClient.request(method, url, req -> {
			final HttpClientRequest proxyRequest = req.options(NettyPipeline.SendOptions::flushOnEach)
					.headers(httpHeaders)
					.chunkedTransfer(chunkedTransfer)
					.failOnServerError(false)
					.failOnClientError(false);

			if (preserveHost) {
				String host = request.getHeaders().getFirst(HttpHeaders.HOST);
				proxyRequest.header(HttpHeaders.HOST, host);
			}

			if (properties.getResponseTimeout() != null) {
				proxyRequest.context(ctx -> ctx.addHandlerFirst(
						new ReadTimeoutHandler(properties.getResponseTimeout().toMillis(), TimeUnit.MILLISECONDS)));
			}

			// Request Body
			return proxyRequest.sendHeaders() //I shouldn't need this
					.send(request.getBody().map(dataBuffer ->
							((NettyDataBuffer) dataBuffer).getNativeBuffer()));
		});

		return responseMono.doOnNext(res -> {
			ServerHttpResponse response = exchange.getResponse();

			// put headers and status so filters can modify the response
			HttpHeaders headers = new HttpHeaders();

			res.responseHeaders().forEach(entry -> headers.add(entry.getKey(), entry.getValue()));

			String contentTypeValue = headers.getFirst(HttpHeaders.CONTENT_TYPE);
			if (StringUtils.hasLength(contentTypeValue)) {
				exchange.getAttributes().put(ORIGINAL_RESPONSE_CONTENT_TYPE_ATTR, contentTypeValue);
			}

			HttpStatus status = HttpStatus.resolve(res.status().code());
			if (status != null) {
				response.setStatusCode(status);
			} else if (response instanceof AbstractServerHttpResponse) {
				// https://jira.spring.io/browse/SPR-16748
				((AbstractServerHttpResponse) response).setStatusCodeValue(res.status().code());
			} else {
				throw new IllegalStateException("Unable to set status code on response: " +res.status().code()+", "+response.getClass());
			}

			// make sure headers filters run after setting status so it is available in response
			HttpHeaders filteredResponseHeaders = HttpHeadersFilter.filter(
					getHeadersFilters(), headers, exchange, Type.RESPONSE);

			response.getHeaders().putAll(filteredResponseHeaders);

			// Defer committing the response until all route filters have run
			// Put client response as ServerWebExchange attribute and write response later NettyWriteResponseFilter
			exchange.getAttributes().put(CLIENT_RESPONSE_ATTR, res);
		})
				.onErrorMap(t -> properties.getResponseTimeout() != null && t instanceof ReadTimeoutException,
						t -> new TimeoutException("Response took longer than timeout: " +
								properties.getResponseTimeout()))

				.then(chain.filter(exchange));
	}

}

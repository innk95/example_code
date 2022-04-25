async def test_dropout_prepare_v1(
    client, order_factory, drop_prep_data, drop_prepare_perfect_answer
):
    order_id, products_data = await order_factory()
    product = products_data[0]
    postgres: Database = client.app['postgres']
    codes = [mark for mark in product['marks']]

    # Находим товары, чьи марки мы хотим вывести оз оборота
    gtins_to_dropout = {code[2:16] for code in codes}
    gtins_in_database = await postgres.fetch_all(
        query=sa.select([products.c.is_closed]).where(
            (products.c.order_id == order_id) & (products.c.gtin.in_(gtins_to_dropout))
        )
    )
    # Проверяем, все ли товары нашлись
    assert len(gtins_in_database) == len(gtins_to_dropout)

    # Проверяем, существуют ли марки
    marks_arr = await postgres.fetch_all(
        sa.select([marks.c.mark]).where(
            (marks.c.mark.in_(product['marks']))
            & (marks.c.status != MarkStatuses.DROPPED)
        )
    )
    # Проверка, не вышли какие-нибудь марки уже из оборота
    assert len(marks_arr) == len(codes)

    # Подготавливаем данные
    drop_prep_data['products'] = []
    for item in marks_arr:
        elem = {'product_tax': '2', 'product_cost': '99', 'code': item['mark']}
        drop_prep_data['products'].append(elem)
    resp = await client.post(
        f'v1/orders/{order_id}/marks/dropout/prepare', json=drop_prep_data
    )
    assert resp.status == 200
    answer = await resp.json()

    # Проверяем с ожидаемыми данными по 1ому продукту product[0]
    assert answer.get('document', False)
    assert answer['document'] == drop_prepare_perfect_answer['document']


async def test_dropout_v1(mis_client_mock, client, order_factory, drop_prep_data):
    order_id, products = await order_factory()
    product = products[0]

    # Подготавливаем документ
    drop_prep_data['products'] = []
    for mark in product['marks']:
        elem = {'product_tax': '2', 'product_cost': '99', 'code': mark}
        drop_prep_data['products'].append(elem)
    # Получаем наш json
    resp = await client.post(
        f'v1/orders/{order_id}/marks/dropout/prepare', json=drop_prep_data
    )
    assert resp.status == 200
    prepared_document = (await resp.json())['document']
    # Засылаем его в ИСМП
    mis_client_mock.return_value = {'GUID': '9abd3d41-76bc-4542-a88e-b1f7be8130b5'}
    await client.post(
        f'v1/orders/{order_id}/marks/dropout',
        json={
            'document': b64encode(dumps(prepared_document)).decode(),
            'signature': b64encode(b'WoWSuchSignAtureVerYSecuRe').decode(),
        },
    )

    # А изменился ли статус?
    postgres: Database = client.app['postgres']
    marks_statuses = await postgres.fetch_all(
        sa.select([marks.c.status]).where(marks.c.mark.in_(product['marks']))
    )
    assert all([mark['status'] == MarkStatuses.DROPPED for mark in marks_statuses])

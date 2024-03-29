import pytest_asyncio
import pytest
from bson import ObjectId

from chouodm.document import DynamicCollectionDocument
from chouodm.query import Q
from chouodm.aggregate.expressions import Sum, Max, Min, Avg, Count


product_collection = "dynamic_product_collection"
image_collection = "dynamic_image_collection"


class Product(DynamicCollectionDocument):
    title: str
    cost: float
    quantity: int
    product_type: str
    config: dict


class ProductImage(DynamicCollectionDocument):
    url: str
    product_id: ObjectId


@pytest_asyncio.fixture(scope="session", autouse=True)
async def innert_producs(event_loop):
    from random import randint

    product_types = {1: "phone", 2: "book", 3: "food"}
    data = [
        Product.New(
            title=str(i),
            cost=float(i),
            quantity=i,
            product_type=product_types[2] if i != 4 else product_types[1],
            config={"type_id": i},
        )
        for i in range(1, 5)
    ]
    await Product.Q(product_collection).insert_many(data)
    products = [p._id for p in await Product.Q(product_collection).find()]
    for product_id in products:
        for i in range(1, randint(2, 6)):
            _ = await ProductImage.New(
                url=f"https://image.com/image-{i}",
                product_id=product_id,
            ).save(image_collection)
    yield
    await Product.Q(product_collection).drop_collection(force=True)
    await ProductImage.Q(image_collection).drop_collection(force=True)


@pytest.mark.asyncio
async def test_aggregation_math_operation(connection):
    max_ = await Product.Q(product_collection).simple_aggregate(aggregation=Max("cost"))
    assert max_.data == {"cost__max": 4}

    min_ = await Product.Q(product_collection).simple_aggregate(aggregation=Min("cost"))
    assert min_.data == {"cost__min": 1}

    sum_ = await Product.Q(product_collection).simple_aggregate(aggregation=Sum("cost"))
    assert sum_.data == {"cost__sum": 10}

    avg_ = await Product.Q(product_collection).simple_aggregate(aggregation=Avg("cost"))
    assert avg_.data == {"cost__avg": 2.5}

    simple_avg = await Product.Q(product_collection).aggregate_sum("cost")
    assert simple_avg == 10.0

    simple_max = await Product.Q(product_collection).aggregate_max("cost")
    assert simple_max == 4

    simple_min = await Product.Q(product_collection).aggregate_min("cost")
    assert simple_min == 1

    simple_avg = await Product.Q(product_collection).aggregate_avg("cost")
    assert simple_avg == 2.5


@pytest.mark.asyncio
async def test_aggregation_multiply(connection):
    result_sum = await Product.Q(product_collection).simple_aggregate(
        aggregation=[Sum("cost"), Sum("quantity")]
    )
    assert result_sum.data == {"cost__sum": 10.0, "quantity__sum": 10}

    result_max = await Product.Q(product_collection).simple_aggregate(
        aggregation=[Max("cost"), Max("quantity")]
    )
    assert result_max.data == {"cost__max": 4.0, "quantity__max": 4}

    result_min = await Product.Q(product_collection).simple_aggregate(
        aggregation=[Min("cost"), Min("quantity")]
    )
    assert result_min.data == {"cost__min": 1.0, "quantity__min": 1}

    result_avg = await Product.Q(product_collection).simple_aggregate(
        aggregation=(Avg("cost"), Avg("quantity"))
    )
    assert result_avg.data == {"cost__avg": 2.5, "quantity__avg": 2.5}

    result_multiply = await Product.Q(product_collection).simple_aggregate(
        aggregation=(Avg("cost"), Max("quantity"))
    )
    assert result_multiply.data == {"cost__avg": 2.5, "quantity__max": 4}

    result_count = await Product.Q(product_collection).simple_aggregate(
        aggregation=Count("product_type")
    )
    assert result_count.data == {"book": {"count": 3}, "phone": {"count": 1}}

    result_count_agg = await Product.Q(product_collection).simple_aggregate(
        aggregation=[Count("product_type"), Sum("cost")]
    )
    assert result_count_agg.data == {
        "book": {"cost__sum": 6.0, "count": 3},
        "phone": {"cost__sum": 4.0, "count": 1},
    }

    result_sum_and_avg_agg_with_group = await Product.Q(
        product_collection
    ).simple_aggregate(
        aggregation=[Avg("cost"), Sum("cost")],
        group_by=["product_type"],
    )
    assert result_sum_and_avg_agg_with_group.data == {
        "phone": {"cost__avg": 4.0, "cost__sum": 4.0},
        "book": {"cost__avg": 2.0, "cost__sum": 6.0},
    }

    result_group_by_by_inners = await Product.Q(product_collection).simple_aggregate(
        group_by=["config.type_id"], aggregation=Count("_id")
    )
    assert result_group_by_by_inners.data == {
        "4": {"count": 1},
        "3": {"count": 1},
        "2": {"count": 1},
        "1": {"count": 1},
    }

    result_sum_and_avg_agg_with_group_many = await Product.Q(
        product_collection
    ).simple_aggregate(
        aggregation=[Avg("cost"), Sum("cost")],
        group_by=["product_type", "quantity"],
    )
    assert result_sum_and_avg_agg_with_group_many.data == {
        "phone|4": {"cost__avg": 4.0, "cost__sum": 4.0},
        "book|3": {"cost__avg": 3.0, "cost__sum": 3.0},
        "book|2": {"cost__avg": 2.0, "cost__sum": 2.0},
        "book|1": {"cost__avg": 1.0, "cost__sum": 1.0},
    }

    result_agg = await Product.Q(product_collection).simple_aggregate(
        aggregation=[Avg("cost"), Max("quantity")]
    )
    assert result_agg.data == {"cost__avg": 2.5, "quantity__max": 4}

    result_not_match_agg = await Product.Q(product_collection).simple_aggregate(
        Q(title__ne="not_match") & Q(title__startswith="not"),
        aggregation=[Avg("cost"), Max("quantity")],
    )
    assert result_not_match_agg.data == {}

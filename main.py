from fastapi import FastAPI
from pydantic import BaseModel
import asyncio
import time

app = FastAPI()


class CalculateRequest(BaseModel):
    numbers: list[int]
    delays: list[float]


class ResultItem(BaseModel):
    number: int
    square: int
    delay: float
    time: float


class CalculateResponse(BaseModel):
    results: list[ResultItem]
    total_time: float
    parallel_faster_than_sequential: bool


async def calculate_square(number: int, delay: float) -> ResultItem:
    start = time.time()
    await asyncio.sleep(delay)
    square = number ** 2
    elapsed = round(time.time() - start, 2)
    return ResultItem(number=number, square=square, delay=delay, time=elapsed)


@app.post("/calculate/", response_model=CalculateResponse)
async def calculate(request: CalculateRequest):
    start_total = time.time()

    tasks = [
        calculate_square(num, delay)
        for num, delay in zip(request.numbers, request.delays)
    ]

    results = await asyncio.gather(*tasks)

    total_time = round(time.time() - start_total, 2)
    sequential_time = sum(request.delays)

    return CalculateResponse(
        results=list(results),
        total_time=total_time,
        parallel_faster_than_sequential=total_time < sequential_time
    )
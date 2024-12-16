import json, os
from typing import List, Optional
from fastapi import FastAPI, Query, HTTPException, Request
# from app.routers import budget_expense, quadrimester_expense, local_activities, 
from app.routers import dashboard, quadrimester_expense, budget_expense, local_activities
from app.utils import TokenBucket
from starlette.middleware.base import BaseHTTPMiddleware
from collections import defaultdict
from fastapi.middleware.cors import CORSMiddleware

# A dictionary to store a TokenBucket for each client with blocking duration 15 sec and refill_rate 1 token/s with 5 tokens capacity 
buckets = defaultdict(lambda: TokenBucket(capacity=35, refill_rate=1, block_duration=15))

# A middleware class to limit API Calls for every client with IP Address 
class RateLimiterMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)

    async def dispatch(self, request: Request, call_next):
        # Identify the client based on IP or user
        client_ip = request.client.host

        # Use a separate token bucket for each client
        # buckets --> defaultdict(<function <lambda> at 0x7ba1f67113f0>, {'192.168.x.x': <app.utils.TokenBucket object at 0x7ba1f6718eb0>})
        bucket = buckets[client_ip] # <app.utils.TokenBucket object at 0x7ba1f6718eb0>

        if bucket.take_token():
            # If a token is available, proceed with the request to routes or other middlewares
            return await call_next(request)
        # If no tokens are available, return a 429 error (rate limit exceeded)
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

app = FastAPI()

# Middleware to restrict Dos Attack 
app.add_middleware(RateLimiterMiddleware) 

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers for separate functionality
# app.include_router(quadrimester_expense.router)
app.include_router(budget_expense.router)
app.include_router(local_activities.router)
app.include_router(quadrimester_expense.router)
app.include_router(dashboard.router)
# app.include_router(idms.router)
# app.include_router(lekbeshi.router)
# app.include_router(tulsipur.router)

@app.get("/")
def read_root(request: Request):
    
    return {"message": f"Welcome to the FastAPI application! {request.client.host}"}





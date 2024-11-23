// DB Assignment 5
// Brittany Klose
// 9/20/24 


/*
   Query 1: Over how many years was the unemployment data collected?
   ===================================================================
*/
db.Unemployment.aggregate(
    {
        $group: {_id: "$Year"}
    }, 
    {
        $count: "unique_year"
    })

// Result: 27

/*
   Query 2: How many states were reported on in this dataset?
   ==========================================================

*/
db.Unemployment.aggregate(
    {
        $group: {_id: "$State"}
    }, 
    {
        $count: "unique_state"
    })

//Result: 47


/*
   Query 3: What does this query compute?

db.unemployment.find({Rate : {$lt: 1.0}}).count()
   =======================================
*/
// Result: 0



/*
   Query 4: Find all counties with unemployment rate higher than 10%
   =========================================================================
  
*/

    db.Unemployment.aggregate(
        {
            $match: {Rate: {$gt: 10.0}}
        }, 
        {
            $group: { _id: "$County"}
        })



/*
   Query 5: Calculate the average unemployment rate across all states.
   =========================================================================
*/

    db.Unemployment.aggregate(
    {
     $project: 
     {
        Rate: { $ifNull: ["$Rate", 0] } 
     }},
     {
        $group:
        {
            _id: null,
            averageRate: 
            {
            $avg: "$Rate"
            }
        }
     }
    )



/*
   Query 6: Find all counties with an unemployment rate between 5% and 8%.
   ===========================================================================

*/
db.Unemployment.aggregate(
    {
      $match: {
        Rate: { $gte: 5.0, $lte: 8.0 }  // Filter where rate is between 5% and 8%
      }
    },
    {
      $group: { _id: "$County" }  
    }
  )





/*
   Query 7: Find the state with the highest unemployment rate. Hint. Use { $limit: 1 }
   ================================================================================

*/
    db.Unemployment.aggregate(
    {
        $project:
        {
            State: 1,
            Rate: 1
        }
     },
    {
        $sort:
        {
            Rate: -1
        }
    },
    {
        $limit:
        1
    }
)



/*
   Query 8: Count how many counties have an unemployment rate above 5%.
   ====================================================================================
*/
db.Unemployment.aggregate(
{
    $project: 
    {
        _id: 1, 
        County: 1, 
        Rate: 1
    }
}, 
{
    $match: 
        {
            Rate: {$gt: 5.0}
        }

}, 
{
    $count:
    
        'County'
}
)



/*
   Query 9: Calculate the average unemployment rate per state by year.
   =================================================================================================

*/

db.Unemployment.aggregate(
    { $project: 
        {
            _id: 0, 
            State: 1, 
            Year:1, 
            Rate: {$ifNull: ["$Rate", 0]}
          }
    },
    {
        $group: 
        {
            _id: {State: "$State", Year: "$Year"},
            avgRatw: {$avg: "$Rate"}
          }
    } )



/*
   Query 10: For each state, calculate the total unemployment rate across all counties (sum of all county rates).
   =================================================================================================
*/
db.Unemployment.aggregate(
    { $project: 
        {
            State: 1,  
            County,
            Rate: {$ifNull: ["$Rate", 0]}
          }
    },
    {
        $group: 
        {
            _id: {State: "$State"},
            sumRates: {$sum: "$Rate"}
          }
    } )



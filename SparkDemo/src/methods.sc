val nums = Array(1,2,3,4,5,6,9,8,77,55,33,22,11,-22,-44)
//val names = Array("venu","rk","sandeep","ali","uma","sundaram")
//scala methods, spark transformations * actions
//map: apply a logic/function on top of each & every element
//in map input elements and output elements lengh same

val fun = (x:Int) => if(x>0 && x%2==0) x*x else x
//x is temp element to do something process

nums.map(fun)
//nums.map(x=>x*x)
//filter ... based on boolean/true value apply a logic on top of each & every element
val even = (x:Int) => x%2==0
nums.filter(x=>x>9).map(x=>x*x)
val names = Array("venu","rk","sandeep","ali","uma","sundaram")

names.map(x=>x.getBytes())





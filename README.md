### wai

#### Overview

Geolocation over social networks

Implementation "spatial label propagation", per:

    http://cs.stanford.edu/~jurgens/docs/compton-jurgens-allen_bigdata-2014.pdf

__TLDR__: Users w/ geolocation are predicted to live at the "average location" of their messages.  Users w/o geolocation are predicted to live at the "average location" of their neighbors.

#### Usage

See `./run.sh`

#### Notes

The "spatial median" of a set is the point that minimizes distance to all other points.  In other places, it's implemented as the _member of the set_ that minimizes distance to all other points.  In practice I don't think it matters, though the latter is significantly faster to implement.  The former doesn't have a closed form, so we have to use an iterative method.

There are a number of ways that this could be extended.  Namely, this implementation weights all of the edges equally.  You could imagine learning a function `f` that uses information about two users to learn a weight, and
and then predict a user's location to be  the point that minimizes the _weighted_ sum of distances to neighbors. That is,
for `user_1`, predict their location to be `m` s.t. `m` minimizes:
```
    sum([f(user_1, neighbor) * distance(m, location(neighbor)) for neighbor in neighbors(user_1)])
```

`f` could use information about each user separately (eg degree) or about the relationship between the users (eg frequency and content of communication).

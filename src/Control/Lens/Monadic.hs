{-# LANGUAGE Rank2Types #-}
module Control.Lens.Monadic where

import Control.Applicative   ((<$>))
import Data.Functor.Identity (Identity(Identity),runIdentity)
import Data.Traversable      (Traversable,mapM)
import Prelude hiding (mapM)

type MLens m s t a b
  = forall f. (Traversable f) => (a -> f b) -> (s -> m (f t))

type MLens' m s a
  = MLens m s s a a

mkMLens :: (Monad m) => (s -> m a) -> (s -> b -> m t)
  -> MLens m s t a b
mkMLens g s f x = g x >>= mapM (s x) . f

overM :: (Functor m) => MLens m s t a b -> (a -> b) -> (s -> m t)
overM l f s = runIdentity <$> l (Identity . f) s

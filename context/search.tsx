import { createContext, useState, useContext } from 'react';

type SearchContextType = {
  searchValue: string;
  setSearchValue:(c: string) => void
}

const SearchContext = createContext<SearchContextType>({
  searchValue: '',
  setSearchValue: () => {},
});

export function useSearch() {
  return useContext(SearchContext);
}

interface Props {
  children: React.ReactNode;
}

export function SearchProvider({ children }: Props) {
  const [searchValue, setSearchValue] = useState('');

  return (
    <SearchContext.Provider value={{ searchValue, setSearchValue }}>
      {children}
    </SearchContext.Provider>
  );
}
